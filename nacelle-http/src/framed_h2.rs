//! Low-level transport for binary envelopes carried in HTTP/2 DATA frames.
//!
//! The caller owns protocol negotiation, listener admission and shutdown. Unlike
//! the HTTP/1 server, this driver does not interpret methods, statuses or bodies.

use bytes::Bytes;
use h2::server::{Builder, SendResponse};
use h2::{Error, Reason, RecvStream};
use http::Request;
use std::future::Future;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::JoinSet;

/// Serve one already-selected h2 connection on the current Tokio runtime.
/// The builder controls h2 settings (including stream and flow-control windows).
/// Handlers own stream flow control, payload limits, timeouts and END_STREAM.
/// Up to 128 handler tasks may run per connection; excess streams are refused.
/// Dropping the driver or disconnecting cancels remaining handler tasks.
pub async fn serve_connection<I, H, F>(io: I, builder: &Builder, mut handle: H) -> Result<(), Error>
where
    I: AsyncRead + AsyncWrite + Unpin,
    H: FnMut(Request<RecvStream>, SendResponse<Bytes>) -> F,
    F: Future<Output = ()> + Send + 'static,
{
    let mut connection = builder.handshake(io).await?;
    let mut handlers = JoinSet::new();
    while let Some(stream) = connection.accept().await {
        let (request, mut respond) = stream?;
        while let Some(result) = handlers.try_join_next() {
            if let Err(error) = result {
                tracing::warn!(%error, "h2 stream handler failed");
            }
        }
        if handlers.len() >= 128 {
            respond.send_reset(Reason::REFUSED_STREAM);
        } else {
            handlers.spawn(handle(request, respond));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};

    fn request() -> Request<()> {
        Request::post("https://localhost/").body(()).unwrap()
    }

    #[tokio::test]
    async fn refuses_excess_stream_without_dispatch_and_reuses_slot() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (peer, io) = tokio::io::duplex(8192);
            let (started, mut gates) = mpsc::unbounded_channel();
            let server = tokio::spawn(async move {
                // Keep the h2 SETTINGS limit above the driver's task limit.
                let mut builder = Builder::new();
                builder.max_concurrent_streams(256);
                serve_connection(io, &builder, move |_, mut respond| {
                    let (release, wait) = oneshot::channel::<()>();
                    started.send(release).unwrap();
                    async move {
                        wait.await.unwrap();
                        respond
                            .send_response(http::Response::new(()), true)
                            .unwrap();
                    }
                })
                .await
            });
            let (mut client, connection) = h2::client::handshake(peer).await.unwrap();
            let driver = tokio::spawn(connection);
            let mut pending = Vec::new();
            for _ in 0..128 {
                client = client.ready().await.unwrap();
                let (response, _) = client.send_request(request(), true).unwrap();
                pending.push((gates.recv().await.unwrap(), response));
            }
            client = client.ready().await.unwrap();
            let (overflow, _) = client.send_request(request(), true).unwrap();
            assert_eq!(
                overflow.await.unwrap_err().reason(),
                Some(Reason::REFUSED_STREAM)
            );
            assert!(matches!(
                gates.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));

            let (release, response) = pending.pop().unwrap();
            release.send(()).unwrap();
            assert_eq!(response.await.unwrap().status(), 200);
            client = client.ready().await.unwrap();
            let (replacement, _) = client.send_request(request(), true).unwrap();
            gates.recv().await.unwrap().send(()).unwrap();
            assert_eq!(replacement.await.unwrap().status(), 200);
            for (release, response) in pending {
                release.send(()).unwrap();
                assert_eq!(response.await.unwrap().status(), 200);
            }
            drop(client);
            driver.await.unwrap().unwrap();
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }

    async fn assert_handler_cancelled(disconnect_peer: bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (peer, io) = tokio::io::duplex(8192);
            let (started, mut observed) = mpsc::unbounded_channel();
            let server = tokio::spawn(async move {
                serve_connection(io, &Builder::new(), move |_, response| {
                    let started = started.clone();
                    async move {
                        let (alive, dropped) = oneshot::channel::<()>();
                        started.send(dropped).unwrap();
                        std::future::pending::<()>().await;
                        drop((response, alive));
                    }
                })
                .await
            });
            let (mut client, connection) = h2::client::handshake(peer).await.unwrap();
            let driver = tokio::spawn(connection);
            let (_response, _) = client.send_request(request(), true).unwrap();
            let dropped = observed.recv().await.unwrap();
            if disconnect_peer {
                driver.abort();
                assert!(driver.await.unwrap_err().is_cancelled());
                let _connection_result = server.await.unwrap();
            } else {
                server.abort();
                assert!(server.await.unwrap_err().is_cancelled());
                driver.abort();
            }
            assert!(dropped.await.is_err());
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn peer_disconnect_drops_live_handler() {
        assert_handler_cancelled(true).await;
    }

    #[tokio::test]
    async fn dropping_driver_drops_live_handler() {
        assert_handler_cancelled(false).await;
    }

    // Envelope transports need byte-exact DATA even when higher-level HTTP
    // frameworks would suppress it. Exercise the peer, not just the encoder.
    #[tokio::test]
    async fn delivers_binary_envelopes_on_independent_streams() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (peer, io) = tokio::io::duplex(8192);
            let server = tokio::spawn(async move {
                serve_connection(io, &Builder::new(), |request, mut respond| async move {
                    let status = match request.uri().path() {
                        "/delete" => 204,
                        "/conditional" => 304,
                        _ => 400,
                    };
                    let mut body = request.into_body();
                    let mut data = Vec::new();
                    while let Some(chunk) = body.data().await {
                        let chunk = chunk.unwrap();
                        body.flow_control().release_capacity(chunk.len()).unwrap();
                        data.extend_from_slice(&chunk);
                    }
                    let head = http::Response::builder().status(status).body(()).unwrap();
                    respond
                        .send_response(head, false)
                        .unwrap()
                        .send_data(data.into(), true)
                        .unwrap();
                })
                .await
                .unwrap();
            });
            let (mut client, connection) = h2::client::handshake(peer).await.unwrap();
            let driver = tokio::spawn(connection);
            for (path, expected) in [("delete", 204), ("conditional", 304)] {
                client = client.ready().await.unwrap();
                let req = Request::post(format!("https://localhost/{path}"))
                    .body(())
                    .unwrap();
                let (response, mut tx) = client.send_request(req, false).unwrap();
                let envelope = Bytes::from_static(b"\x00\xff\x10");
                tx.send_data(envelope.clone(), true).unwrap();
                let response = response.await.unwrap();
                assert_eq!(response.status().as_u16(), expected);
                let mut rx = response.into_body();
                assert_eq!(rx.data().await.unwrap().unwrap(), envelope);
                assert!(rx.data().await.is_none());
            }
            drop(client);
            driver.await.unwrap().unwrap();
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}
