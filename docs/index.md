<p class="nacelle-logo">
	<img src="assets/nacelle-mark.svg" width="132" alt="Nacelle logo">
</p>

# nacelle documentation

nacelle is an experimental Tokio-based Rust library for streaming application
handlers across TCP and HTTP transports.

This book is the narrative documentation site. It follows the same broad
delivery model as the Rust Book: chapter-oriented Markdown, search, keyboard
navigation, and a local/offline build. Its content is organized like Django's
documentation:

- **Tutorials** take you through a working path.
- **Topic guides** explain concepts and design choices.
- **How-to guides** solve specific operational tasks.
- **Reference** pages document exact behavior and APIs.

Rust API reference is still generated separately with `cargo doc`.

## Start here

If you are new to nacelle, read:

1. [Getting started](tutorials/getting-started.md)
2. [Architecture](topics/architecture.md)
3. [Configure production limits](how-to/configure-production.md)

If you are validating performance, read:

1. [Run the stress harness](tutorials/stress-harness.md)
2. [Compare performance profiles](how-to/compare-performance.md)
3. [Performance model](topics/performance-model.md)

Internal readiness plans and assessments live under `docs/internal` and are not
part of this public book.

