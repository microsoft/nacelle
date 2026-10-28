---
name: Communication style
description: "Use throughout Nacelle work when writing responses, reviews, plans, change descriptions, comments, or other authored prose."
applyTo: "**"
---

# Engineering communication style

Apply this guidance throughout repository work, not only documentation edits.
Follow the technical-contract precedence in the
[repository instructions](../../copilot-instructions.md#agent-workflows-and-style).

## Rules

### VOICE-001: Lead with the result

State the answer, outcome, finding, or action first, followed by the rationale,
constraints, and evidence needed to assess it. Distinguish observations,
inferences, requirements, proposals, and unresolved decisions.

Why: the reader should immediately understand the conclusion and its basis.

### VOICE-002: Use direct and concrete language

Use familiar words, active voice, specific nouns, and observable behavior.
Remove filler, ceremony, generic machine phrasing, unsupported certainty, and
qualifiers such as "real," "actual," or "supported" that add no useful distinction.

Why: concrete wording makes claims understandable and assessable.

### VOICE-006: Preserve established terminology

Use one term per concept and preserve requester/project vocabulary and spelling
unless clarity requires correction. A **boundary** separates responsibilities,
an **interface** defines an interaction contract, and a **layer** groups
responsibilities with defined dependency directions. Name components and behavior;
explain specialized terms and add a glossary only when useful.

Why: consistent terms keep prose and implementation on the same model.

### VOICE-007: Respect confirmed wording restrictions

Do not use "seam," "materially," or "load bearing" in authored prose, including
hyphenated forms and regardless of case. Name the boundary, responsibility,
dependency, or consequence instead, or omit filler. Preserve exact quotations
and identifiers where required and allow discussion of these terms themselves.
Do not infer additional word or character bans from another repository.

Why: authored prose should follow confirmed preferences without inventing new ones.

## Preferences

### VOICE-101: Match structure and detail to the reader

Prefer concise connected paragraphs; use lists for parallel items, sequences, or
comparisons and headings for navigation. Retain useful assumptions and consequences,
not repeated summaries. Write warmly and candidly without flattery, exaggerated
enthusiasm, canned transitions, or defensive explanations.

Why: brevity comes from choosing relevant information, not omitting evidence.

### VOICE-104: Minimize process narration

Report findings, decisions, changes in direction, and the next uncertainty rather
than routine commands. Mention unchanged behavior only when asked or consequential.

Why: progress reporting should help the reader judge the work, not replay it.
