# Typed pipeline prototype

This unpublished crate validates the proposed request-context, responder,
middleware, and shared/local handler contracts before they enter a published
Nacelle crate.

## Proven contracts

- One generic `RequestContext<Request, Responder, State>` owns exactly one
  concrete responder.
- A TCP responder can borrow a connection-local write buffer.
- An HTTP responder can produce a concrete Hyper-compatible response.
- The same generic middleware shape wraps TCP, HTTP, shared-runtime, and local
  handlers.
- Shared-runtime handlers return `Send` futures and require `Send` request
  state.
- Local handlers may return `!Send` futures and use worker-local state.
- The prototype contains no trait objects, boxed futures, or boxed responders.

## Future representation decision

Use return-position `impl Future` in traits (RPITIT) for the initial contract.
It is stable on the workspace Rust 1.95 toolchain, allows ordinary `async fn`
implementations, and keeps the concrete future hidden without allocation.

An associated `Future` type works naturally for responders whose completion is
already a named future such as `Ready`. It is substantially less ergonomic for
general async handlers and generic middleware on stable Rust because the future
created by an `async fn` is not nameable, and `impl Trait` in associated types
is not stable. A GAT can express lifetimes but does not make that async future
nameable. The practical alternatives are handwritten future types, boxing, or
an additional macro, none of which improves this initial contract.

Revisit the choice only if the production transport integration exposes a
borrowing pattern RPITIT cannot express or compiler/code-size measurements show
a material problem.