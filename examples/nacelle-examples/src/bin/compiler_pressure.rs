#![recursion_limit = "256"]

use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use nacelle::NacelleApp;
use nacelle::core::lifecycle::NacelleDrainDeadline;
use nacelle::core::pipeline::{ConnectionInfo, handler_fn, local_handler_fn};
use nacelle::core::{
    NacelleBody, NacelleConnectionMeta, NacelleError, NacelleRuntimeState, NacelleShutdown,
    NacelleTelemetry,
};
use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer};
use nacelle::tcp::{
    FrameBuffer, LocalTcpServer, NacelleTcpOptions, Protocol, SerialHandlerAdapter,
    SerialTcpHandler, SerialTcpRequestContext, SerialTcpServer, TcpCompletion,
    TcpConnectionAdmission, TcpHandlerCompletion, TcpRequestContext, TcpResponse, TcpServer,
    connection,
};
use nacelle_reference_protocol::LengthDelimitedProtocol;

const MAX_FUTURE_BYTES: usize = 16 * 1024;
type FutureSizes = [(&'static str, usize); 10];

struct SerialHandler;

impl<P: Protocol<Response = TcpResponse, ConnectionState: Send>> SerialTcpHandler<P>
    for SerialHandler
{
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, P>,
    ) -> Result<TcpHandlerCompletion<P>, NacelleError> {
        context.respond(TcpResponse::empty()).await
    }
}

struct InlineStateProtocol;

impl Protocol for InlineStateProtocol {
    type Request = <LengthDelimitedProtocol as Protocol>::Request;
    type Response = TcpResponse;
    type ConnectionState = [u8; 6 * 1024];
    type Decoder = <LengthDelimitedProtocol as Protocol>::Decoder;
    type ResponseContext = <LengthDelimitedProtocol as Protocol>::ResponseContext;
    type ErrorContext = <LengthDelimitedProtocol as Protocol>::ErrorContext;

    fn decoder(&self, max_frame_len: usize) -> Self::Decoder {
        LengthDelimitedProtocol.decoder(max_frame_len)
    }

    fn connection_state(&self, _: &ConnectionInfo) -> Self::ConnectionState {
        [0; 6 * 1024]
    }

    fn request_wire_bytes(&self, request: &Self::Request, body_len: usize) -> usize {
        LengthDelimitedProtocol.request_wire_bytes(request, body_len)
    }

    fn response_context(&self, request: &Self::Request) -> Self::ResponseContext {
        LengthDelimitedProtocol.response_context(request)
    }

    fn error_context(&self, request: &Self::Request) -> Self::ErrorContext {
        LengthDelimitedProtocol.error_context(request)
    }

    fn apply_response(&self, context: &mut Self::ResponseContext, response: &Self::Response) {
        LengthDelimitedProtocol.apply_response(context, response);
    }

    fn max_response_frame_overhead(&self) -> usize {
        LengthDelimitedProtocol.max_response_frame_overhead()
    }

    fn response_body(&self, response: Self::Response) -> NacelleBody {
        LengthDelimitedProtocol.response_body(response)
    }

    fn encode_response_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        LengthDelimitedProtocol.encode_response_chunk(context, chunk, dst)
    }

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        LengthDelimitedProtocol.encode_response_terminal_chunk(context, chunk, dst)
    }

    fn encode_response_end(
        &self,
        context: &mut Self::ResponseContext,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        LengthDelimitedProtocol.encode_response_end(context, dst)
    }

    fn error_response(
        &self,
        context: Option<&Self::ErrorContext>,
        error: &NacelleError,
    ) -> Option<TcpCompletion<Self::Response, Self::ResponseContext>> {
        LengthDelimitedProtocol.error_response(context, error)
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

    let (_admitted_shared_client, admitted_shared_io) = tokio::io::duplex(64);
    let shared_admission =
        shared_connection_server.admit_connection(NacelleConnectionMeta::tcp(None, None))?;
    let admitted_shared =
        shared_connection_server.serve_io_with_admission(admitted_shared_io, shared_admission);

    let (_admitted_serial_client, mut admitted_serial_io) = tokio::io::duplex(64);
    let serial_admission =
        serial_server.admit_connection(NacelleConnectionMeta::tcp(None, None))?;
    let admitted_serial =
        serial_server.serve_io_with_admission(&mut admitted_serial_io, serial_admission);

    let (_direct_serial_client, mut direct_serial_io) = tokio::io::duplex(64);
    let runtime = NacelleRuntimeState::default();
    let telemetry = NacelleTelemetry::default();
    let direct_admission =
        TcpConnectionAdmission::new(&runtime, &telemetry, NacelleConnectionMeta::tcp(None, None))?;
    let direct_serial = connection::serve_serial_stream_with_admission(
        &mut direct_serial_io,
        Arc::new(LengthDelimitedProtocol),
        Arc::new(SerialHandlerAdapter(SerialHandler)),
        Default::default(),
        telemetry.clone(),
        runtime.clone(),
        Default::default(),
        direct_admission,
    );

    let (_inline_client, mut inline_io) = tokio::io::duplex(64);
    let inline_admission =
        TcpConnectionAdmission::new(&runtime, &telemetry, NacelleConnectionMeta::tcp(None, None))?;
    let inline_serial = connection::serve_serial_stream_with_admission(
        &mut inline_io,
        Arc::new(InlineStateProtocol),
        Arc::new(SerialHandlerAdapter(SerialHandler)),
        Default::default(),
        telemetry,
        runtime,
        Default::default(),
        inline_admission,
    );

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
        ("shared-admission", std::mem::size_of_val(&admitted_shared)),
        ("serial-admission", std::mem::size_of_val(&admitted_serial)),
        (
            "direct-serial-admission",
            std::mem::size_of_val(&direct_serial),
        ),
        (
            "inline-state-serial-admission",
            std::mem::size_of_val(&inline_serial),
        ),
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
    for (name, size) in &sizes {
        println!("{name}: {size} bytes");
    }
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
        println!("{sizes:?}");
        verify_future_sizes(&sizes).expect("representative futures should stay below the ceiling");
    }
}
