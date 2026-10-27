use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use tokio::sync::mpsc;

use crate::error::NacelleError;
use crate::telemetry::NacelleTransport;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NacelleConnectionTlsMeta {
    pub provider: &'static str,
    pub protocol: Option<String>,
    pub cipher_suite: Option<String>,
    pub cipher_bits: Option<u16>,
    pub cipher_algorithm_bits: Option<u16>,
    pub server_name: Option<String>,
}

impl NacelleConnectionTlsMeta {
    pub fn new(provider: &'static str) -> Self {
        Self {
            provider,
            protocol: None,
            cipher_suite: None,
            cipher_bits: None,
            cipher_algorithm_bits: None,
            server_name: None,
        }
    }

    pub fn with_protocol(mut self, protocol: impl Into<String>) -> Self {
        self.protocol = Some(protocol.into());
        self
    }

    pub fn with_cipher_suite(mut self, cipher_suite: impl Into<String>) -> Self {
        self.cipher_suite = Some(cipher_suite.into());
        self
    }

    pub fn with_cipher_bits(mut self, cipher_bits: u16) -> Self {
        self.cipher_bits = Some(cipher_bits);
        self
    }

    pub fn with_cipher_algorithm_bits(mut self, cipher_algorithm_bits: u16) -> Self {
        self.cipher_algorithm_bits = Some(cipher_algorithm_bits);
        self
    }

    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = Some(server_name.into());
        self
    }
}

#[non_exhaustive]
#[derive(Clone)]
pub struct NacelleConnectionMeta {
    pub connection_id: u64,
    pub transport: NacelleTransport,
    pub listener: Arc<str>,
    pub peer_addr: Option<SocketAddr>,
    pub peer_ip: Option<IpAddr>,
    pub local_addr: Option<SocketAddr>,
    pub local_path: Option<PathBuf>,
    pub tls: Option<NacelleConnectionTlsMeta>,
}

impl fmt::Debug for NacelleConnectionMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NacelleConnectionMeta")
            .field("connection_id", &self.connection_id)
            .field("transport", &self.transport)
            .field("listener", &self.listener)
            .field("peer_addr", &self.peer_addr)
            .field("peer_ip", &self.peer_ip)
            .field("local_addr", &self.local_addr)
            .field("local_path", &self.local_path)
            .field("tls", &self.tls)
            .finish()
    }
}

impl NacelleConnectionMeta {
    pub fn tcp(peer_addr: Option<SocketAddr>, local_addr: Option<SocketAddr>) -> Self {
        Self {
            connection_id: next_connection_id(),
            transport: NacelleTransport::new("tcp"),
            listener: default_listener(),
            peer_ip: peer_addr.map(|addr| addr.ip()),
            peer_addr,
            local_addr,
            local_path: None,
            tls: None,
        }
    }

    pub fn unix_socket(local_path: Option<PathBuf>) -> Self {
        Self {
            connection_id: next_connection_id(),
            transport: NacelleTransport::new("unix_socket"),
            listener: default_listener(),
            peer_addr: None,
            peer_ip: None,
            local_addr: None,
            local_path,
            tls: None,
        }
    }

    pub fn http(peer_ip: Option<IpAddr>) -> Self {
        Self {
            connection_id: next_connection_id(),
            transport: NacelleTransport::new("http"),
            listener: default_listener(),
            peer_addr: None,
            peer_ip,
            local_addr: None,
            local_path: None,
            tls: None,
        }
    }

    pub fn http_socket(peer_addr: Option<SocketAddr>, local_addr: Option<SocketAddr>) -> Self {
        Self {
            connection_id: next_connection_id(),
            transport: NacelleTransport::new("http"),
            listener: default_listener(),
            peer_ip: peer_addr.map(|addr| addr.ip()),
            peer_addr,
            local_addr,
            local_path: None,
            tls: None,
        }
    }

    pub fn with_tls(mut self, tls: NacelleConnectionTlsMeta) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_listener(mut self, listener: impl Into<Arc<str>>) -> Self {
        self.listener = listener.into();
        self
    }

    pub fn tls_label(&self) -> &'static str {
        self.tls.as_ref().map_or("none", |tls| tls.provider)
    }

    pub fn with_connection_id(mut self, connection_id: u64) -> Self {
        self.connection_id = connection_id;
        self
    }
}

fn next_connection_id() -> u64 {
    NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

fn default_listener() -> Arc<str> {
    Arc::from("direct")
}

enum NacelleBodySource {
    // Single-chunk bodies (the common case for small payloads): avoids Vec/Box heap alloc.
    SingleChunk(Option<Bytes>),
    Buffered {
        chunks: Box<[Bytes]>,
        next_index: usize,
    },
    Streaming {
        receiver: BodyReceiver,
    },
}

enum BodyReceiver {
    Public(mpsc::Receiver<Result<Bytes, NacelleError>>),
}

/// A buffered or streaming request or response body.
pub struct NacelleBody {
    source: NacelleBodySource,
    remaining_bytes: usize,
}

impl NacelleBody {
    #[doc(hidden)]
    pub fn new(
        receiver: mpsc::Receiver<Result<Bytes, NacelleError>>,
        remaining_bytes: usize,
    ) -> Self {
        Self {
            source: NacelleBodySource::Streaming {
                receiver: BodyReceiver::Public(receiver),
            },
            remaining_bytes,
        }
    }

    pub fn empty() -> Self {
        Self {
            source: NacelleBodySource::Buffered {
                chunks: Box::new([]),
                next_index: 0,
            },
            remaining_bytes: 0,
        }
    }

    pub fn bytes(chunk: impl Into<Bytes>) -> Self {
        let chunk = chunk.into();
        let remaining_bytes = chunk.len();
        if remaining_bytes == 0 {
            return Self::empty();
        }
        Self {
            source: NacelleBodySource::SingleChunk(Some(chunk)),
            remaining_bytes,
        }
    }

    pub fn channel(capacity: usize) -> (mpsc::Sender<Result<Bytes, NacelleError>>, NacelleBody) {
        Self::channel_with_remaining(capacity, 0)
    }

    #[doc(hidden)]
    pub fn channel_with_remaining(
        capacity: usize,
        remaining_bytes: usize,
    ) -> (mpsc::Sender<Result<Bytes, NacelleError>>, NacelleBody) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (tx, NacelleBody::new(rx, remaining_bytes))
    }

    #[doc(hidden)]
    pub fn from_single_chunk(chunk: Bytes, remaining_bytes: usize) -> Self {
        Self {
            source: NacelleBodySource::SingleChunk(Some(chunk)),
            remaining_bytes,
        }
    }

    #[doc(hidden)]
    pub fn from_buffered(chunks: Vec<Bytes>, remaining_bytes: usize) -> Self {
        Self {
            source: NacelleBodySource::Buffered {
                chunks: chunks.into_boxed_slice(),
                next_index: 0,
            },
            remaining_bytes,
        }
    }

    #[doc(hidden)]
    pub fn try_into_single_chunk_or_empty(self) -> Result<Option<Bytes>, Self> {
        let Self {
            source,
            remaining_bytes,
        } = self;
        match source {
            NacelleBodySource::SingleChunk(chunk) => Ok(chunk),
            NacelleBodySource::Buffered { chunks, next_index } => {
                let remaining = chunks.len().saturating_sub(next_index);
                if remaining == 0 {
                    Ok(None)
                } else if remaining == 1 {
                    Ok(Some(chunks[next_index].clone()))
                } else {
                    Err(Self {
                        source: NacelleBodySource::Buffered { chunks, next_index },
                        remaining_bytes,
                    })
                }
            }
            NacelleBodySource::Streaming { receiver } => Err(Self {
                source: NacelleBodySource::Streaming { receiver },
                remaining_bytes,
            }),
        }
    }

    pub fn remaining_bytes(&self) -> usize {
        self.remaining_bytes
    }

    /// Return the next body chunk.
    pub async fn next_chunk(&mut self) -> Option<Result<Bytes, NacelleError>> {
        let Self {
            source,
            remaining_bytes,
        } = self;
        match source {
            NacelleBodySource::SingleChunk(slot) => {
                let chunk = slot.take()?;
                *remaining_bytes = 0;
                Some(Ok(chunk))
            }
            NacelleBodySource::Buffered { chunks, next_index } => {
                let chunk = chunks.get(*next_index)?.clone();
                *next_index += 1;
                *remaining_bytes = remaining_bytes.saturating_sub(chunk.len());
                Some(Ok(chunk))
            }
            NacelleBodySource::Streaming { receiver } => match receiver {
                BodyReceiver::Public(receiver) => match receiver.recv().await {
                    Some(Ok(chunk)) => {
                        *remaining_bytes = remaining_bytes.saturating_sub(chunk.len());
                        Some(Ok(chunk))
                    }
                    other => other,
                },
            },
        }
    }
}

impl Stream for NacelleBody {
    type Item = Result<Bytes, NacelleError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Self {
            source,
            remaining_bytes,
        } = self.get_mut();
        match source {
            NacelleBodySource::SingleChunk(slot) => {
                let Some(chunk) = slot.take() else {
                    return Poll::Ready(None);
                };
                *remaining_bytes = 0;
                Poll::Ready(Some(Ok(chunk)))
            }
            NacelleBodySource::Buffered { chunks, next_index } => {
                let Some(chunk) = chunks.get(*next_index).cloned() else {
                    return Poll::Ready(None);
                };
                *next_index += 1;
                *remaining_bytes = remaining_bytes.saturating_sub(chunk.len());
                Poll::Ready(Some(Ok(chunk)))
            }
            NacelleBodySource::Streaming { receiver } => match receiver {
                BodyReceiver::Public(receiver) => match receiver.poll_recv(cx) {
                    Poll::Ready(Some(Ok(chunk))) => {
                        *remaining_bytes = remaining_bytes.saturating_sub(chunk.len());
                        Poll::Ready(Some(Ok(chunk)))
                    }
                    other => other,
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn public_channel_preserves_known_remaining_bytes() {
        let (sender, mut body) = NacelleBody::channel_with_remaining(1, 5);
        sender.send(Ok(Bytes::from_static(b"hello"))).await.unwrap();
        drop(sender);

        assert_eq!(body.remaining_bytes(), 5);
        assert_eq!(body.next_chunk().await.unwrap().unwrap(), b"hello"[..]);
        assert_eq!(body.remaining_bytes(), 0);
    }

    use super::*;

    #[test]
    fn unix_socket_connection_meta_sets_transport_and_path() {
        let path = PathBuf::from("/tmp/nacelle.sock");
        let meta = NacelleConnectionMeta::unix_socket(Some(path.clone()));

        assert_ne!(meta.connection_id, 0);
        assert_eq!(meta.transport, NacelleTransport::new("unix_socket"));
        assert_eq!(meta.local_path, Some(path));
        assert_eq!(meta.peer_addr, None);
        assert_eq!(meta.peer_ip, None);
        assert_eq!(meta.local_addr, None);
    }

    #[test]
    fn connection_meta_assigns_stable_unique_ids() {
        let first = NacelleConnectionMeta::tcp(None, None);
        let second = NacelleConnectionMeta::tcp(None, None);
        let first_clone = first.clone();

        assert_ne!(first.connection_id, 0);
        assert_ne!(first.connection_id, second.connection_id);
        assert_eq!(first.connection_id, first_clone.connection_id);
    }

    #[test]
    fn tls_meta_records_cipher_strength() {
        let meta = NacelleConnectionTlsMeta::new("openssl")
            .with_protocol("TLSv1.3")
            .with_cipher_suite("TLS_AES_256_GCM_SHA384")
            .with_cipher_bits(256)
            .with_cipher_algorithm_bits(256);

        assert_eq!(meta.provider, "openssl");
        assert_eq!(meta.protocol.as_deref(), Some("TLSv1.3"));
        assert_eq!(meta.cipher_suite.as_deref(), Some("TLS_AES_256_GCM_SHA384"));
        assert_eq!(meta.cipher_bits, Some(256));
        assert_eq!(meta.cipher_algorithm_bits, Some(256));
    }
}
