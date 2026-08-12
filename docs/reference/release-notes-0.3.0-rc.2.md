# 0.3.0-rc.2 release and rollback notes

RC.2 is the second `0.3.0` release candidate. It supersedes the published RC.1
candidate for stabilization evidence because post-RC.1 correctness coverage and
an observable metrics-schema correction required another prerelease.

## Included changes

- Add external-consumer and internal regressions that keep representative
  serving futures below the 16 KiB compiler-pressure ceiling without a crate
  recursion override.
- Add direct correctness evidence for concurrent per-peer admission, by-value
  cancellation, fragmented frame boundaries, unknown-length body limits,
  shutdown ordering, connection-task panic supervision, and repeated listener
  lifecycle cleanup.
- Align metrics to singular, resource-first names and base units. Request and
  response body sizes are histograms rather than cumulative counters.
- Add an enforced dependency policy for advisories, licenses, sources,
  wildcard requirements, and duplicate versions.
- Preserve RC.1 Rust APIs, wire behavior, resource-limit defaults, timeout
  defaults, and graph-wide TLS backend exclusivity.

Metric consumers must follow
[Migrate from 0.3.0-rc.1 to 0.3.0-rc.2](../how-to/migrate-rc-2.md).
Applications upgrading from beta.5 must also follow
[Migrate from 0.3.0-beta.5 to 0.3.0-rc.1](../how-to/migrate-rc-1.md).

## Rollback

RC.1 remains published and is the rollback target for an RC.2 deployment:

```toml
nacelle = { version = "=0.3.0-rc.1" }
```

Use the same feature set and TLS backend as the RC.2 deployment. Rebuild the
consumer from a locked dependency graph, redeploy it through the normal service
rollback mechanism, and restore RC.1 metric queries or recording rules at the
same time. Do not leave RC.2 body-size histogram queries attached to RC.1 byte
counters, or mix RC.1 millisecond duration samples with RC.2 second samples.

No data or wire-format migration is required. If rollback is caused by a generic
runtime defect, reduce it to a transport-neutral regression before resuming the
stable release. Publish RC.3 rather than replacing or retagging RC.2 when a
candidate correction is required.

This rollback guidance does not claim production readiness for every workload.
Validate RC.2 against the intended service limits, TLS mode, observability
backend, and deployment rollback path before promotion.
