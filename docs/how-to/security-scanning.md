# Run security scans

Run vulnerability and dependency checks before release:

```bash
cargo audit
cargo deny check
cargo tree -i serde_yaml
cargo tree -i unsafe-libyaml
```

`deny.toml` rejects known advisories, unapproved licenses and sources, wildcard
dependencies, and duplicate versions that do not have a dependency-owner
rationale. Review every exception when updating the lockfile; do not add a
blanket advisory, license, or duplicate-version exception.

`serde_yaml` and `unsafe-libyaml` should not appear in the dependency tree.


