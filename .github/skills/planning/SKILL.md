---
name: planning
description: "Plan broad or risky Nacelle work. Use for architecture changes, migrations, release or production-readiness assessments, security hardening, performance baselines, documentation restructuring, validation strategies, or multi-session work."
---

# Planning Nacelle Work

Turn substantial goals into scoped, ordered work with explicit risks, validation gates, and handoff-ready checkpoints.

## Scope

- Inspect the relevant repository state, architecture documentation, manifests, modules, tests, configuration, scripts, CI, and recent commits before planning.
- Use chat for narrow plans. Create or update a durable plan for broad, risky, architectural, release, migration, or multi-session work when the user requests one or the work needs a persistent handoff.
- Continue the nearest existing plan when it matches the goal. If no convention exists and a durable plan is justified, use `.github/plans/YYYY-MM-DD-<topic>.md`.
- Keep internal assessments and decisions separate from public operator, API, user, and release documentation.
- Leave implementation tactics to the owning crate and nearby code unless they affect ordering, risk, validation, compatibility, or architecture.
- When the user requests only a plan, do not implement or commit changes.

## Procedure

1. Name the target: feature, bug class, migration, architecture change, release, readiness level, performance claim, documentation deliverable, or checkpoint series.
2. State the affected users and environments, constraints, and non-goals.
3. Summarize the relevant current state and identify dependencies or compatibility boundaries.
4. Order phases by risk: discovery before design, tests before risky behavior changes, compatibility before migration, measurement before optimization, and documentation before public readiness claims.
5. Give each phase a goal, concrete tasks, acceptance criteria, validation evidence, and rollback or mitigation where relevant.
6. Record risks and unknowns together with the first measurement or inspection that can reduce each one.
7. Update durable plans as facts change, preserving useful decisions and marking superseded assumptions clearly.

## Durable Plan Format

Include only sections that help execute or hand off the work:

- purpose and scope
- target users, environments, or release/readiness level
- constraints and non-goals
- current state and dependencies
- ordered phases
- acceptance and validation gates
- risks, unknowns, and mitigations
- rollback, checkpoint, or handoff slices

## Validation Planning

- Derive commands and gates from `.github/copilot-instructions.md`, the applicable `.github/instructions/*.instructions.md` files, `.github/workflows/ci.yml`, and repository scripts instead of maintaining a second generic command list.
- Choose the smallest focused check that can falsify each behavioral claim, then broaden validation according to the change's blast radius.
- Report correctness, security, performance, operability, compatibility, and documentation evidence separately; one gate does not prove the others.
- For performance claims, specify the platform, toolchain, feature flags, configuration, workload, dataset, baseline, and comparison criteria.
- For security or production-readiness work, include trust boundaries, resource limits, dependency risk, observability, rollback, and recovery where relevant.

## Readiness Language

Do not describe Nacelle as production ready without scope. Prefer concrete conclusions such as:

- ready for internal users after the named gates pass
- safe for beta behind an explicit rollback mechanism
- ready for a measured low-risk workload with rollback documented
- not yet ready for public or regulated workloads
- requires a performance rebaseline before release

## Checkpoints

When checkpoint commits are requested, define coherent slices such as plan or documentation, tests, architecture boundary, behavior change, migration, security control, performance profile, validation, or release preparation. Each checkpoint must build or test at the level required by its risk. Use Conventional Commits, and never create commits unless the user explicitly requests them.