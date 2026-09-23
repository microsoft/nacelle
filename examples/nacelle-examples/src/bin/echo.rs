//! Opcode 1 echoes the body; opcode 2 consumes it and completes silently.
//! Other opcodes fail: the reference protocol's `error_response` turns the
//! error text into a typed response with the error flag and original request ID.

use bytes::BytesMut;
use nacelle::core::pipeline::handler_fn;
use nacelle::prelude::*;
use nacelle::tcp::{TcpHandlerCompletion, TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

async fn echo(
    mut context: TcpRequestContext<LengthDelimitedProtocol>,
) -> Result<TcpHandlerCompletion<LengthDelimitedProtocol>, NacelleError> {
    let opcode = context.request().head.opcode;
    let mut echoed = BytesMut::new();
    while let Some(chunk) = context.request_mut().body.next_chunk().await {
        echoed.extend_from_slice(&chunk?);
    }
    match opcode {
        1 => context.respond(TcpResponse::bytes(echoed.freeze())).await,
        2 => Ok(context.complete()),
        _ => Err(NacelleError::handler(std::io::Error::other(format!(
            "unknown opcode {opcode}"
        )))),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler_fn(echo))
        .build()?;

    println!("nacelle echo server listening on {addr}");
    NacelleApp::new()
        .with_ctrl_c_shutdown()
        .tcp("echo", addr, server)
        .run()
        .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nacelle_reference_protocol::{FRAME_FLAG_END, FRAME_FLAG_ERROR, FRAME_FLAG_START};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    async fn exchange(requests: &[u8]) -> (Vec<u8>, Result<(), NacelleError>) {
        tokio::time::timeout(Duration::from_secs(2), async {
            let server = TcpServer::<LengthDelimitedProtocol>::builder()
                .protocol(LengthDelimitedProtocol)
                .handler(handler_fn(echo))
                .build()
                .expect("echo server");
            let (mut client, io) = tokio::io::duplex(1024);
            let task = tokio::spawn(async move { server.serve_io(io).await });
            client.write_all(requests).await.expect("write requests");
            client.shutdown().await.expect("close request stream");
            let mut output = Vec::new();
            client
                .read_to_end(&mut output)
                .await
                .expect("read responses");
            (output, task.await.expect("server task"))
        })
        .await
        .expect("exchange should finish")
    }

    #[tokio::test]
    async fn silent_completion_does_not_emit_a_frame_or_stop_the_next_request() {
        let protocol = LengthDelimitedProtocol;
        let mut input = BytesMut::new();
        input.extend_from_slice(
            &protocol
                .encode_request_frame(7, 2, 0, b"silent")
                .expect("silent request"),
        );
        input.extend_from_slice(
            &protocol
                .encode_request_frame(8, 1, 0, b"echo")
                .expect("echo request"),
        );

        let (output, result) = exchange(&input).await;
        result.expect("requests should complete");
        let expected = protocol
            .encode_request_frame(8, 1, FRAME_FLAG_START | FRAME_FLAG_END, b"echo")
            .expect("expected echo frame");
        assert_eq!(output.as_slice(), expected.as_ref());
    }

    #[tokio::test]
    async fn handler_error_preserves_the_reference_error_frame() {
        let protocol = LengthDelimitedProtocol;
        let input = protocol
            .encode_request_frame(9, 99, 0, b"")
            .expect("unknown opcode request");
        let error = NacelleError::handler(std::io::Error::other("unknown opcode 99"));
        let (output, result) = exchange(&input).await;
        assert!(result.is_err());
        let expected = protocol
            .encode_request_frame(
                9,
                99,
                FRAME_FLAG_START | FRAME_FLAG_END | FRAME_FLAG_ERROR,
                error.to_string().as_bytes(),
            )
            .expect("expected error frame");
        assert_eq!(output.as_slice(), expected.as_ref());
    }

    #[tokio::test]
    async fn decoder_error_uses_zero_request_metadata() {
        let protocol = LengthDelimitedProtocol;
        let error = NacelleError::InvalidFrame("frame length is smaller than the fixed header");
        let (output, result) = exchange(&[0, 0, 0, 0]).await;
        assert!(result.is_err());
        let expected = protocol
            .encode_request_frame(
                0,
                0,
                FRAME_FLAG_START | FRAME_FLAG_END | FRAME_FLAG_ERROR,
                error.to_string().as_bytes(),
            )
            .expect("expected decoder error frame");
        assert_eq!(output.as_slice(), expected.as_ref());
    }
}
