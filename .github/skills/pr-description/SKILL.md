---
name: pr-description
description: "Draft Nacelle PR titles, pull request descriptions, commit titles, commit descriptions, and squash commit messages. Use when asked for PR or commit wording, a branch summary, or a PR description that can be used in a commit. Omit validation sections from commit bodies and remove filler."
---

# Drafting PR and Commit Text

Produce copyable text grounded in the requested change. Drafting does not
authorize creating a PR, committing, pushing, or modifying source files.

Follow the [repository instructions](../../copilot-instructions.md) and general
[communication](../../instructions/style/communication-style.instructions.md)
and [documentation](../../instructions/style/documentation-style.instructions.md)
styles.

## Establish the Scope

1. Use the user's requested branch, commit range, staged changes, or PR. For a
   whole-branch description, inspect the diff against the target branch and the
   commits in that range, not just the latest commit or conversation.
2. Check the working-tree state. Distinguish committed changes from staged or
   unstaged changes; do not imply uncommitted work is already in a PR.
3. Use an existing local target ref, normally `origin/main`, unless the user
   specifies another. Do not fetch or change branches just to draft text.
4. Read enough of the affected implementation and documentation to establish
   behavior, rationale, compatibility consequences, and migration requirements.
   Treat commit messages and previous summaries as claims to verify.
5. If the target ref or necessary evidence is unavailable, state that limitation
   outside the draft. Do not invent changes, motivations, or successful checks.

## Titles

- Use a concise Conventional Commit title:
  `<type>(<optional-scope>): <concrete result>`.
- Choose the type from the main outcome: `fix`, `feat`, `refactor`, `perf`,
  `docs`, `test`, or `chore`. Name an owning crate or subsystem when a scope
  improves clarity; omit it for cross-cutting changes.
- Use an imperative verb and no trailing period. Avoid vague titles such as
  "update code" or "address feedback."
- Use `!` for an intentional breaking change, and explain the required caller
  migration in the description or a `BREAKING CHANGE:` footer.
- A PR and its squash commit can share a title. Use different titles only when
  their scopes differ.

## PR Description

- Lead with the change and its reason. Summarize the resulting behavior rather
  than the order in which files were edited.
- Use a short paragraph or a few parallel bullets. Include compatibility,
  configuration, operational consequences, and migration instructions when
  relevant. Omit empty headings and unrelated follow-up work.
- Include validation evidence only when useful, requested, or required by an
  existing PR template. Distinguish checks run from checks not run and disclose
  relevant failures; never infer that CI passed.
- If the user wants the PR description reused as a commit body, apply the
  commit-description rules instead. Put any necessary check results or remaining
  validation failures in a separate note, not in the copyable body.

## Commit Description

- Explain the concrete change and why it is needed. Retain consequential design
  choices, compatibility details, and migration instructions.
- Omit the entire validation section. Do not replace it with a `Testing`,
  `Checks`, or `Verification` section, test counts, command transcripts, CI
  status, or a paragraph saying checks passed.
- Describing added or changed tests is allowed when those tests are part of the
  change; reporting their execution results is not part of the commit body.
- Keep necessary validation evidence and known gate failures in the PR
  description or a separate handoff note. Omitting them from a commit body does
  not authorize hiding failures or claiming readiness.
- Include a `BREAKING CHANGE:` footer when needed. Preserve required issue
  references and trailers when preparing a complete commit message.

## Remove Filler

- Do not write "this is a real issue" or similar assurances. Name the triggering
  condition, incorrect behavior, and consequence instead.
- Remove introductory ceremony, self-praise, repeated summaries, unsupported
  certainty, and modifiers that add no technical distinction.
- Describe observable behavior instead of calling a change "robust,"
  "comprehensive," "proper," or "actual" without explaining what those words
  distinguish. Preserve identifiers, quotations, and terms with a technical
  meaning.
- Do not make performance, security, or production-readiness claims without
  evidence. Prefer an accurate description of the change to a sales claim.

## Output

Return only the requested fields. When both PR and commit text are requested,
default to a shared **PR / commit title**, a **PR description**, and a separate
**Commit description**. Put each in its own fenced block with labels outside
the blocks. For a complete commit message, combine the title, a blank line, the
body, and any required footers in one block.

For "a PR description that can be used in a commit," return a shared title and
one commit-ready description without a validation section. Keep any necessary
evidence or limitation note outside those blocks. Do not run builds or tests
solely to draft text, and do not add process narration around the deliverables.
