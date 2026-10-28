#![cfg(all(feature = "tcp", feature = "http", feature = "policies"))]

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use nacelle::core::pipeline::handler_fn;
use nacelle::core::policy::{RequestRate, map_input};
use nacelle::core::{NacelleConnectionMeta, NacellePeerRateLimiter, NacelleShutdown};
use nacelle::http::{HttpPolicies, HttpRequestContext, HttpRequestHead, HttpResponse, HyperServer};
use nacelle::tcp::policies::{TcpHead, TcpPolicies};
use nacelle::tcp::{TcpRequestContext, TcpServer};
use nacelle_reference_protocol::{FrameRequest, LengthDelimitedProtocol};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn tcp_peer(input: &TcpHead<'_, FrameRequest, Arc<()>>) -> Option<IpAddr> {
    input.connection().peer_ip
}

fn http_peer(input: &HttpRequestHead<'_>) -> Option<IpAddr> {
    input.peer_ip()
}

#[tokio::test]
async fn tcp_and_http_share_one_initialized_request_rate_budget() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let rate = RequestRate::new(Arc::new(NacellePeerRateLimiter::new(16)), 1);
        let tcp = TcpServer::<LengthDelimitedProtocol>::builder()
            .protocol(LengthDelimitedProtocol)
            .handler(handler_fn(
                |context: TcpRequestContext<LengthDelimitedProtocol>| async move {
                    Ok(context.complete())
                },
            ))
            .policies(TcpPolicies::new().on_request_head(map_input(tcp_peer, rate.clone())))
            .build()
            .unwrap();
        let http = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context
                .respond(HttpResponse::bytes(http::StatusCode::OK, "unexpected"))
                .await
        }))
        .with_policies(HttpPolicies::new().on_request_head(map_input(http_peer, rate)));

        let (mut client, io) = tokio::io::duplex(128);
        let frame = LengthDelimitedProtocol
            .encode_request_frame(1, 1, 0, &[])
            .unwrap();
        client.write_all(&frame).await.unwrap();
        client.shutdown().await.unwrap();
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        tcp.serve_io_with_connection_meta(io, NacelleConnectionMeta::tcp(Some(peer), None))
            .await
            .unwrap();

        let (shutdown, token) = NacelleShutdown::pair();
        let serving = tokio::spawn(http.serve_listener_with_shutdown(listener, token));
        let mut connection = tokio::net::TcpStream::connect(addr).await.unwrap();
        connection
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        connection.read_to_end(&mut response).await.unwrap();
        shutdown.shutdown();
        serving.await.unwrap().unwrap();
        assert!(
            response.starts_with(b"HTTP/1.1 429"),
            "shared rate budget must include TCP requests"
        );
    })
    .await
    .expect("cross-transport policy test must finish");
}
