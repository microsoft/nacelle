# Dependency security resolutions

This page records fixes for published dependency advisories. It is not an
exhaustive advisory inventory or a substitute for running the
[security scans](../how-to/security-scanning.md) against the current lockfile.
Dependency manifests and the lockfile remain authoritative for version
requirements and resolved versions.

When addressing a dependency advisory, add or update its entry here with the
affected dependency, advisory link, previous version, selected fixed version,
remediation, and verification performed. Keep advisory-specific version notes
out of general scanning instructions. Do not record an advisory as resolved
merely because it has been ignored or suppressed.

## RUSTSEC-2026-0386: Tokio AsyncFd I/O safety

- Advisory: [RUSTSEC-2026-0386](https://rustsec.org/advisories/RUSTSEC-2026-0386).
- Dependency: `tokio`.
- Previous locked version: `1.52.3`.
- Resolution: raised the workspace minimum and locked version to `1.53.3`,
  which includes the advisory's upstream fix.
- Scope: the advisory concerns I/O safety in Unix `AsyncFd` constructors.
  No application-level workaround or advisory suppression was added.
- Verification at resolution: `cargo audit` and `cargo deny check advisories`
  both passed with the updated lockfile.

Rerun the scans when updating dependencies; this resolution record does not
establish that later dependency sets are free of advisories.
