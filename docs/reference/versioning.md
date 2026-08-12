# Versioning and support

Nacelle uses semantic versioning for published crates. This policy defines how
new release lines are promoted, how patch releases are maintained, and which
minor lines receive support.

## Major and minor releases

Every new major or minor release line progresses through three stages:

1. Publish one or more beta versions, such as `0.4.0-beta.1` and
   `0.4.0-beta.2`, while the release scope and public contracts stabilize.
2. Promote the beta line to a release candidate, such as `0.4.0-rc.1`, when it
   is ready for final compatibility and release validation. Publish another
   release candidate if the candidate requires changes.
3. Promote a validated release candidate to the release version, such as
   `0.4.0`.

This promotion path applies to both major and minor releases. Beta and release
candidate versions are prereleases for evaluation and stabilization; they do
not add a supported minor line to the support window.

## Patch releases

Patch releases do not repeat the beta and release-candidate progression. Apply
a compatible patch directly to the applicable supported release branch and to
`main` when the fix is still relevant there, then publish the next patch
version. For example, a fix for `0.4.0` is released directly as `0.4.1`.

A patch may differ on the release branch and `main` when later development has
changed the affected code, but both changes must preserve the fix's behavior.

## Supported versions

Nacelle supports the current released minor line and the immediately preceding
released minor line. This is the N-1 support window. Support begins with
`0.3.0`; every version before `0.3.0` is unsupported.

The support window therefore develops as follows:

- When `0.3` is current, only the `0.3` line is supported.
- When `0.4` is current, the `0.4` and `0.3` lines are supported.
- When `0.5` is current, the `0.5` and `0.4` lines are supported, and `0.3` is
  unsupported.

Support applies to minor lines, not every patch within them. Consumers should
run the latest available patch release in a supported minor line. Once a minor
line falls outside the N-1 window, it no longer receives fixes or releases.