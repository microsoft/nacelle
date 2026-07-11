use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use http::StatusCode;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{NacelleError, NacelleLimits, NacelleRuntimeState, NacelleShutdown};
use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer, NacelleHttpLimits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MEMORY_LIMIT: usize = 64 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let bind_addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:0".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let addr = listener.local_addr()?;

    let runtime_state = NacelleRuntimeState::new(
        NacelleLimits::default()
            .with_max_memory_bytes(MEMORY_LIMIT)
            .with_max_request_body_bytes(MEMORY_LIMIT)
            .with_memory_allocation_timeout(Duration::from_millis(100)),
    );
    let server_state = runtime_state.clone();
    let (shutdown, token) = NacelleShutdown::pair();
    let server = HyperServer::new(handler_fn(
        |mut context: HttpRequestContext<()>| async move {
            let mut bytes = 0_usize;
            while let Some(chunk) = context.request_mut().body.next_chunk().await {
                bytes += chunk?.len();
            }
            context
                .respond(HttpResponse::bytes(
                    StatusCode::OK,
                    format!("accepted {bytes} bytes\n"),
                ))
                .await
        },
    ))
    .with_runtime_state(server_state)
    .with_http_limits(NacelleHttpLimits::default().with_keep_alive(false));
    let server_task =
        tokio::spawn(async move { server.serve_listener_with_shutdown(listener, token).await });

    println!("memory guard demo listening on {addr}");
    println!("configured memory budget: {MEMORY_LIMIT} bytes");

    let mut held = tokio::net::TcpStream::connect(addr).await?;
    write_post_headers(&mut held, MEMORY_LIMIT).await?;
    wait_for_memory(&runtime_state, MEMORY_LIMIT).await?;
    println!(
        "first upload allocated the full budget: {} bytes in use",
        runtime_state.memory_used_bytes()
    );

    let rejected = one_shot_post(addr, 1).await?;
    expect(
        rejected.starts_with("HTTP/1.1 500 Internal Server Error")
            && rejected.contains("memory_allocation"),
        "second request should be rejected while memory is exhausted",
    )?;
    println!(
        "second upload while budget is full: {}",
        response_status_line(&rejected)
    );

    held.write_all(&vec![b'a'; MEMORY_LIMIT]).await?;
    let first_response = read_response(held).await?;
    expect(
        first_response.starts_with("HTTP/1.1 200 OK"),
        "first held upload should complete",
    )?;
    wait_for_memory(&runtime_state, 0).await?;
    println!(
        "first upload finished; memory returned to {} bytes",
        runtime_state.memory_used_bytes()
    );

    let recovered = one_shot_post(addr, MEMORY_LIMIT).await?;
    expect(
        recovered.starts_with("HTTP/1.1 200 OK"),
        "new request should succeed after memory is released",
    )?;
    println!(
        "next full-budget upload after release: {}",
        response_status_line(&recovered)
    );

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .map_err(|_| NacelleError::Timeout("example_shutdown"))?
        .map_err(NacelleError::from)??;

    Ok(())
}

async fn one_shot_post(addr: SocketAddr, body_len: usize) -> Result<String, NacelleError> {
    let mut client = tokio::net::TcpStream::connect(addr).await?;
    write_post_headers(&mut client, body_len).await?;
    client.write_all(&vec![b'b'; body_len]).await?;
    read_response(client).await
}

async fn write_post_headers(
    client: &mut tokio::net::TcpStream,
    body_len: usize,
) -> Result<(), NacelleError> {
    client
        .write_all(
            format!(
                "POST /upload HTTP/1.1\r\n\
                 Host: localhost\r\n\
                 Content-Length: {body_len}\r\n\
                 Connection: close\r\n\
                 \r\n"
            )
            .as_bytes(),
        )
        .await?;
    Ok(())
}

async fn read_response(mut client: tokio::net::TcpStream) -> Result<String, NacelleError> {
    let mut response = Vec::new();
    client.read_to_end(&mut response).await?;
    String::from_utf8(response).map_err(NacelleError::protocol)
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

fn response_status_line(response: &str) -> &str {
    response.lines().next().unwrap_or("<empty response>")
}

fn expect(condition: bool, message: &'static str) -> Result<(), NacelleError> {
    if condition {
        Ok(())
    } else {
        Err(NacelleError::handler(io::Error::other(message)))
    }
}
