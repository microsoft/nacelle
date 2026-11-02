<!-- BEGIN MICROSOFT SECURITY.MD V1.0.0 BLOCK -->

## Security

Microsoft takes the security of our software products and services seriously, which
includes all source code repositories in our GitHub organizations.

**Please do not report security vulnerabilities through public GitHub issues.**

For security reporting information, locations, contact information, and policies,
please review the latest guidance for Microsoft repositories at
[https://aka.ms/SECURITY.md](https://aka.ms/SECURITY.md).

<!-- END MICROSOFT SECURITY.MD BLOCK -->

## Transport Security

Configure proxy identity, body budgets, and HTTP error handling as described in
the [HTTP hardening reference](docs/reference/http-hardening.md). Applications
own authentication, authorization, and any diagnostics exposed to clients.

Global concurrent connection and connection admission rate limits are unlimited
by default. Set explicit finite budgets before exposing public listeners.
Per-peer connection and HTTP request-rate controls require the opt-in
`peer-limits` feature; they are absent without it. See
[runtime limits](docs/topics/runtime-limits.md) for admission windows and
worker partitioning, and the [migration guidance](docs/reference/api-stability.md#global-and-peer-limit-migration).

Rustls builds require 0.23.46 or newer, which addresses RUSTSEC-2026-0285,
RUSTSEC-2026-0346, RUSTSEC-2026-0347, and RUSTSEC-2026-0349. The Rustls provider
also requires rustls-webpki 0.103.16 or newer to address RUSTSEC-2026-0382.
Tokio requires 1.53.3 or newer to address the AsyncFd unsoundness warning
RUSTSEC-2026-0386. Downstream applications should refresh their lockfiles and
run `cargo audit` when updating Nacelle.

The OpenSSL PEM helper requires TLS 1.2 or newer; custom TLS configurations own
their policy. See [runtime limits](docs/topics/runtime-limits.md) for certificate
reload and memory-accounting guarantees and limitations.