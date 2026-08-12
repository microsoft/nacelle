#![recursion_limit = "256"]

use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use nacelle::NacelleApp;
use nacelle::core::lifecycle::NacelleDrainDeadline;
use nacelle::core::pipeline::{handler_fn, local_handler_fn};
use nacelle::core::{NacelleError, NacelleShutdown};
use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer};
use nacelle::tcp::{
    LocalTcpServer, NacelleTcpOptions, SerialTcpHandler, SerialTcpRequestContext, SerialTcpServer,
    TcpHandlerCompletion, TcpRequestContext, TcpResponse, TcpServer,
};
use nacelle_reference_protocol::LengthDelimitedProtocol;

const MAX_FUTURE_BYTES: usize = 16 * 1024;
type FutureSizes = [(&'static str, usize); 6];

struct SerialHandler;

impl SerialTcpHandler<LengthDelimitedProtocol> for SerialHandler {
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, LengthDelimitedProtocol>,
    ) -> Result<TcpHandlerCompletion<LengthDelimitedProtocol>, NacelleError> {
        context.respond(TcpResponse::empty()).await
    }
}

fn shared_server() -> Result<
    TcpServer<LengthDelimitedProtocol, impl nacelle::tcp::TcpHandler<LengthDelimitedProtocol>>,
    NacelleError,
> {
    TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler_fn(
            |context: TcpRequestContext<LengthDelimitedProtocol>| async move {
                context.respond(TcpResponse::empty()).await
            },
        ))
        .build()
}

fn http_server() -> HyperServer<impl nacelle::http::HttpHandler<()>> {
    HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
        context
            .respond(HttpResponse::empty(http::StatusCode::NO_CONTENT))
            .await
    }))
}

async fn representative_future_sizes() -> Result<FutureSizes, NacelleError> {
    let (_shared_client, shared_io) = tokio::io::duplex(64);
    let shared_connection_server = shared_server()?;
    let shared = shared_connection_server.serve_io(shared_io);

    let (_serial_client, serial_io) = tokio::io::duplex(64);
    let serial_server = SerialTcpServer::new(LengthDelimitedProtocol, SerialHandler);
    let serial = serial_server.serve_io(serial_io);

    let local_server = LocalTcpServer::new(
        LengthDelimitedProtocol,
        local_handler_fn(
            |context: TcpRequestContext<LengthDelimitedProtocol>| async move {
                context.respond(TcpResponse::empty()).await
            },
        ),
    );
    let local_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let (_local_shutdown, local_token) = NacelleShutdown::pair();
    let local = nacelle::advanced::runtime::serve_local_tcp_listener(
        Rc::new(local_server),
        local_listener,
        NacelleTcpOptions::default(),
        local_token,
        NacelleDrainDeadline::new(Duration::from_secs(1)),
    );

    let generated = nacelle::NacelleTlsConfig::self_signed(["localhost"])?;
    let tls = nacelle::advanced::runtime::serve_tcp_tls(
        Arc::new(shared_server()?),
        "127.0.0.1:0".parse().expect("valid probe address"),
        generated.tls_config,
    );

    let http = http_server().serve("127.0.0.1:0".parse().expect("valid probe address"));

    let app = NacelleApp::new()
        .tcp(
            "compiler-pressure-tcp",
            "127.0.0.1:0".parse().expect("valid probe address"),
            shared_server()?,
        )
        .http(
            "compiler-pressure-http",
            "127.0.0.1:0".parse().expect("valid probe address"),
            http_server(),
        )
        .run();

    Ok([
        ("shared", std::mem::size_of_val(&shared)),
        ("serial", std::mem::size_of_val(&serial)),
        ("local", std::mem::size_of_val(&local)),
        ("tls", std::mem::size_of_val(&tls)),
        ("http", std::mem::size_of_val(&http)),
        ("app", std::mem::size_of_val(&app)),
    ])
}

fn verify_future_sizes(sizes: &FutureSizes) -> Result<(), NacelleError> {
    if let Some((name, size)) = sizes.iter().find(|(_, size)| *size >= MAX_FUTURE_BYTES) {
        return Err(NacelleError::protocol(std::io::Error::other(format!(
            "{name} serving future is {size} bytes; ceiling is {MAX_FUTURE_BYTES} bytes"
        ))));
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), NacelleError> {
    let sizes = representative_future_sizes().await?;
    println!("representative serving future sizes: {sizes:?}");
    verify_future_sizes(&sizes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn representative_serving_futures_stay_below_compiler_pressure_ceiling() {
        let sizes = representative_future_sizes()
            .await
            .expect("representative futures should instantiate");
        verify_future_sizes(&sizes).expect("representative futures should stay below the ceiling");
    }
}
