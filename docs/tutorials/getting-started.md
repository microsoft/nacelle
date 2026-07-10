# Getting started

This tutorial gets a minimal TCP service running with the reference
length-delimited protocol.

## Add nacelle

In this workspace, the umbrella crate is `nacelle`. It re-exports the transport
crates and owns the reference protocol:

```rust
use nacelle::prelude::*;
```

## Build a handler

Handlers receive a `NacelleRequest` and return a `NacelleResponse`.

```rust
let handler = handler_fn(|mut request: NacelleRequest| async move {
    while let Some(chunk) = request.body.next_chunk().await {
        let _ = chunk?;
    }

    Ok(NacelleResponse::tcp_bytes("ok"))
});
```

## Start the app

```rust
let addr = "127.0.0.1:8080".parse().map_err(NacelleError::protocol)?;
let protocols = NacelleProtocols::new()
    .tcp::<FrameRequest, _>("echo", addr, LengthDelimitedProtocol);

NacelleApp::new(handler)
    .with_telemetry(NacelleTelemetry::default())
    .with_ctrl_c_shutdown()
    .serve(protocols)
    .await?;
# Ok::<(), NacelleError>(())
```

## Next steps

- Run `cargo run --features reference_protocol --example app_core` to see one
  app core served through multiple protocol adapters.
- Read the [architecture guide](../topics/architecture.md) to understand the request path.
- Read [runtime limits and backpressure](../topics/runtime-limits.md) before raising connection counts.
- Use [Run the stress harness](stress-harness.md) to validate a local build.
