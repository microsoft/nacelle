use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{NacelleLimits, NacelleRuntimeState, NacelleShutdown, NacelleTimeoutReason};
use nacelle::prelude::*;
use nacelle::tcp::{NacelleTcpConfig, TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::{FRAME_FLAG_END, FRAME_FLAG_ERROR, LengthDelimitedProtocol};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MEMORY_LIMIT: usize = 64 * 1024;
const BUFFER_BYTES: usize = 1024;
const CONNECTION_BUFFER_BYTES: usize = BUFFER_BYTES * 2;
const HELD_BODY_BYTES: usize = MEMORY_LIMIT - (CONNECTION_BUFFER_BYTES * 2);
const OPCODE_UPLOAD: u64 = 1;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let bind_addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:18080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;
    if bind_addr.port() == 0 {
        return Err(NacelleError::handler(io::Error::new(
            io::ErrorKind::InvalidInput,
            "tcp_memory_guard requires a nonzero port",
        )));
    }
    let addr = bind_addr;

    let runtime_state = NacelleRuntimeState::new(
        NacelleLimits::default()
            .with_max_memory_bytes(MEMORY_LIMIT)
            .with_max_request_body_bytes(HELD_BODY_BYTES)
            .with_memory_allocation_timeout(Duration::from_millis(100)),
    );
    let config = NacelleTcpConfig::default()
        .with_read_buffer_capacity(BUFFER_BYTES)
        .with_response_buffer_capacity(BUFFER_BYTES)
        .with_max_frame_len(HELD_BODY_BYTES + 20);
    let server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler_fn(
            |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
                let mut bytes = 0_usize;
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    bytes += chunk?.len();
                }
                context
                    .respond(TcpResponse::bytes(format!("accepted {bytes} bytes\n")))
                    .await
            },
        ))
        .tcp_config(config)
        .runtime_state(runtime_state.clone())
        .build()?;
    let (shutdown, token) = NacelleShutdown::pair();
    let server_task =
        tokio::spawn(async move { server.serve_tcp_with_shutdown(bind_addr, token).await });

    println!("tcp memory guard demo listening on {addr}");
    println!("configured memory budget: {MEMORY_LIMIT} bytes");
    println!("per-connection TCP buffers: {CONNECTION_BUFFER_BYTES} bytes");

    let mut held = connect_with_retry(addr).await?;
    held.write_all(&request_header(1, HELD_BODY_BYTES)).await?;
    let held_expected = CONNECTION_BUFFER_BYTES + HELD_BODY_BYTES;
    wait_for_memory(&runtime_state, held_expected).await?;
    println!(
        "first TCP request allocated {HELD_BODY_BYTES} body bytes; {} bytes total in use",
        runtime_state.memory_used_bytes()
    );

    let mut rejected = tokio::net::TcpStream::connect(addr).await?;
    wait_for_memory(&runtime_state, MEMORY_LIMIT).await?;
    rejected.write_all(&request_header(2, 1)).await?;
    let rejected_error = read_tcp_response(rejected)
        .await
        .expect_err("memory-exhausted request should return an error frame");
    expect(
        rejected_error.to_string().contains("memory"),
        "memory-exhausted TCP request should return a memory-limit error frame",
    )?;
    expect(
        runtime_state.memory_used_bytes() <= MEMORY_LIMIT,
        "runtime memory usage must not exceed the configured budget",
    )?;
    wait_for_memory(&runtime_state, held_expected).await?;
    println!(
        "second TCP request was rejected while the budget was full; memory stayed <= {MEMORY_LIMIT}"
    );

    held.write_all(&vec![b'a'; HELD_BODY_BYTES]).await?;
    let first_response = read_tcp_response(held).await?;
    expect(
        first_response == format!("accepted {HELD_BODY_BYTES} bytes\n"),
        "first held TCP request should complete",
    )?;
    wait_for_memory(&runtime_state, 0).await?;
    println!(
        "first TCP request finished; memory returned to {} bytes",
        runtime_state.memory_used_bytes()
    );

    let recovered = one_shot_tcp(addr, 3, HELD_BODY_BYTES).await?;
    expect(
        recovered == format!("accepted {HELD_BODY_BYTES} bytes\n"),
        "new TCP request should succeed after memory is released",
    )?;
    println!("next TCP upload after release: accepted {HELD_BODY_BYTES} bytes");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .map_err(|_| NacelleError::Timeout(NacelleTimeoutReason::Other("example_shutdown")))?
        .map_err(NacelleError::from)??;

    Ok(())
}

async fn one_shot_tcp(
    addr: SocketAddr,
    request_id: u64,
    body_len: usize,
) -> Result<String, NacelleError> {
    let mut client = connect_with_retry(addr).await?;
    client
        .write_all(&request_header(request_id, body_len))
        .await?;
    client.write_all(&vec![b'b'; body_len]).await?;
    read_tcp_response(client).await
}

async fn connect_with_retry(addr: SocketAddr) -> Result<tokio::net::TcpStream, NacelleError> {
    let mut last_error = None;
    for _ in 0..100 {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(last_error
        .map(NacelleError::from)
        .unwrap_or_else(|| NacelleError::UnexpectedEof))
}

fn request_header(request_id: u64, body_len: usize) -> [u8; 24] {
    let mut header = [0_u8; 24];
    let frame_len = 20 + body_len;
    header[0..4].copy_from_slice(&(frame_len as u32).to_le_bytes());
    header[4..12].copy_from_slice(&request_id.to_le_bytes());
    header[12..20].copy_from_slice(&OPCODE_UPLOAD.to_le_bytes());
    header[20..24].copy_from_slice(&0_u32.to_le_bytes());
    header
}

async fn read_tcp_response(mut client: tokio::net::TcpStream) -> Result<String, NacelleError> {
    let mut bytes = BytesMut::new();
    let mut body = BytesMut::new();
    loop {
        while bytes.len() < 4 {
            if client.read_buf(&mut bytes).await? == 0 {
                return Err(NacelleError::UnexpectedEof);
            }
        }
        let frame_len = u32::from_le_bytes(bytes[0..4].try_into().expect("fixed width")) as usize;
        let wire_len = 4 + frame_len;
        while bytes.len() < wire_len {
            if client.read_buf(&mut bytes).await? == 0 {
                return Err(NacelleError::UnexpectedEof);
            }
        }

        let flags = u32::from_le_bytes(bytes[20..24].try_into().expect("fixed width"));
        if flags & FRAME_FLAG_ERROR != 0 {
            return Err(NacelleError::handler(io::Error::other(
                String::from_utf8_lossy(&bytes[24..wire_len]).into_owned(),
            )));
        }
        body.put_slice(&bytes[24..wire_len]);
        drop(bytes.split_to(wire_len));
        if flags & FRAME_FLAG_END != 0 {
            return String::from_utf8(body.to_vec()).map_err(NacelleError::protocol);
        }
    }
}

async fn wait_for_memory(
    runtime_state: &NacelleRuntimeState,
    expected: usize,
) -> Result<(), NacelleError> {
    for _ in 0..100 {
        if runtime_state.memory_used_bytes() == expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(NacelleError::handler(io::Error::other(format!(
        "memory did not reach {expected}; observed {}",
        runtime_state.memory_used_bytes()
    ))))
}

fn expect(condition: bool, message: &'static str) -> Result<(), NacelleError> {
    if condition {
        Ok(())
    } else {
        Err(NacelleError::handler(io::Error::other(message)))
    }
}
