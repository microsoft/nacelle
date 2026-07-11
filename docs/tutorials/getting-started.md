# Getting started

This tutorial gets a minimal TCP service running with the repository's example
length-delimited protocol.

## Add nacelle

In this workspace, the umbrella crate is `nacelle`. The unpublished reference
protocol package is an example consumer of its TCP and codec APIs:

```rust
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{NacelleError, NacelleTelemetry};
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle::NacelleApp;
use nacelle_reference_protocol::LengthDelimitedProtocol;
```

## Build a handler

TCP handlers receive a protocol-specific `TcpRequestContext` and must complete
it through its typed responder.

```rust
let handler = handler_fn(
    |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
    while let Some(chunk) = context.request_mut().body.next_chunk().await {
        let _ = chunk?;
    }

    context.respond(TcpResponse::bytes("ok")).await
});
```

## Start the app

```rust
let addr = "127.0.0.1:8080".parse().map_err(NacelleError::protocol)?;
let server = TcpServer::<LengthDelimitedProtocol>::builder()
    .protocol(LengthDelimitedProtocol)
    .handler(handler)
    .build()?;

NacelleApp::with_telemetry(NacelleTelemetry::default())
    .with_ctrl_c_shutdown()
    .tcp("echo", addr, server)
    .run()
    .await?;
# Ok::<(), NacelleError>(())
```

## Next steps

- Run `cargo run -p nacelle-examples --bin app_core` to see one
  app core served through multiple protocol adapters.
- Read the [architecture guide](../topics/architecture.md) to understand the request path.
- Read [runtime limits and backpressure](../topics/runtime-limits.md) before raising connection counts.
- Use [Run the stress harness](stress-harness.md) to validate a local build.
