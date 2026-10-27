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

## Safe TLS test diagnostics

Certificate and key parsing must be separate from failure reporting, including
in test code. Bind the result, then inspect it with an explicit `match`:

```rust,ignore
let parsed = nacelle_rustls::parse_pem_certificates(certificate_pem.as_bytes());
let certificates = match parsed {
    Ok(certificates) => certificates,
    Err(_) => panic!("test certificate should parse"),
};
```

The HTTP server tests and TCP runtime Rustls tests each provide a test-only
`test_certificate` helper using this pattern. Reuse the helper within its crate;
do not add test panic behavior to the public TLS API.

Avoid these patterns:

- `parse_pem_certificates(...).expect(...)` or `.unwrap()`: a failure formats the
  parser error, which may contain input-derived details.
- `parse_pem_certificates(...).unwrap_or_else(|_| panic!("fixed message"))`:
  although the message is constant, CodeQL has still reported this fluent form
  as a sensitive-data logging path. Use the separate result and `match` instead.
- Printing or debug-formatting PEM, parsed certificates/keys, parsing results,
  or parser errors in logs, panics, or assertions (including `assert_eq!` on
  sensitive values). Test-runner output and CI logs are persistent output too.
- Returning an empty/default certificate after a parse error or suppressing the
  alert. Invalid fixtures must still fail explicitly.

Keep both valid-certificate handshake coverage and malformed/empty-input tests
that check the exact constant panic payload. Rust tests verify behavior and
diagnostics; only a new CodeQL scan can confirm that the alerts are cleared.
Record advisory-specific fixes and verification in
[Dependency security resolutions](../reference/dependency-security.md).
