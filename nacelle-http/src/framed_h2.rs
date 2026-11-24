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
