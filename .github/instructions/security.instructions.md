---
name: Security
description: "Use when changing or reviewing HTTP hardening, TLS, trust boundaries, input validation, resource limits, rate limiting, unsafe code, deserialization, secrets, or dependency security."
---

# Security Guidelines

- Check affected trust boundaries, input validation, transport security, dependency risk, unsafe code, and deserialization behavior.
- Keep authentication and authorization in the application, protocol layer, or edge proxy; Nacelle does not implement them.
- Make stricter public-edge or production controls explicit, documented, and configurable when compatibility or performance could be affected.
- Prefer early rejection, bounded queues and buffers, timeouts, rate limits, and backpressure over unbounded resource growth.
- Use low-cardinality telemetry and stable reason values for rejection, timeout, rate-limit, and policy events. Never emit secrets or unbounded peer-controlled values as labels.
- Preserve provider separation: provider-neutral TLS belongs in `nacelle-core`, Rustls serves HTTP and TCP, and OpenSSL support is TCP-specific.
- Update `SECURITY.md`, `docs/reference/http-hardening.md`, or `docs/how-to/harden-http.md` when security guarantees or operator guidance changes.