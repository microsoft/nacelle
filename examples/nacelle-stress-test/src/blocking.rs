use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use nacelle::core::NacelleError;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{Barrier, watch};
use tokio::task::JoinHandle;

use super::{StressConfig, WorkerResult, run_sequential_worker};

pub(super) trait Transport: Read + Write + Send + Unpin + 'static {
    fn socket(&self) -> &TcpStream;
    fn shutdown_write(&mut self) -> io::Result<()>;
}

impl Transport for TcpStream {
    fn socket(&self) -> &TcpStream {
        self
    }

    fn shutdown_write(&mut self) -> io::Result<()> {
        self.shutdown(Shutdown::Write)
    }
}

#[cfg(feature = "rustls")]
impl Transport for rustls::StreamOwned<rustls::ClientConnection, TcpStream> {
    fn socket(&self) -> &TcpStream {
        &self.sock
    }

    fn shutdown_write(&mut self) -> io::Result<()> {
        self.conn.send_close_notify();
        self.flush()?;
        self.sock.shutdown(Shutdown::Write)
    }
}

// These polls may block and must run only on the connection's blocking worker.
struct BlockingIo<S>(S);

impl<S: Transport> AsyncRead for BlockingIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let limit = buffer.remaining().min(4096);
        if limit == 0 {
            return Poll::Ready(Ok(()));
        }
        let read = self.0.read(buffer.initialize_unfilled_to(limit));
        Poll::Ready(read.map(|bytes| buffer.advance(bytes)))
    }
}

impl<S: Transport> AsyncWrite for BlockingIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.0.write(bytes))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.0.flush())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.0.shutdown_write())
    }
}

struct CancellationGuard {
    socket: TcpStream,
    cancelled: watch::Sender<bool>,
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        self.cancelled.send_replace(true);
        let _ = self.socket.shutdown(Shutdown::Both);
    }
}

pub(super) fn into_std(stream: tokio::net::TcpStream) -> io::Result<TcpStream> {
    let stream = stream.into_std()?;
    stream.set_nonblocking(false)?;
    Ok(stream)
}

fn start<S: Transport>(
    worker_id: usize,
    stream: S,
    config: StressConfig,
    barrier: Arc<Barrier>,
) -> io::Result<(
    CancellationGuard,
    JoinHandle<Result<WorkerResult, NacelleError>>,
)> {
    let socket = stream.socket().try_clone()?;
    let (cancelled, mut cancellation) = watch::channel(false);
    let guard = CancellationGuard { socket, cancelled };
    let worker = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            tokio::select! {
                biased;
                _ = cancellation.wait_for(|cancelled| *cancelled) => {
                    Err(io::Error::new(io::ErrorKind::Interrupted, "blocking stress worker cancelled").into())
                }
                result = run_sequential_worker(worker_id, BlockingIo(stream), &config, &barrier) => result,
            }
        })
    });
    Ok((guard, worker))
}

pub(super) async fn run<S: Transport>(
    worker_id: usize,
    stream: S,
    config: StressConfig,
    barrier: Arc<Barrier>,
) -> Result<WorkerResult, NacelleError> {
    let (_guard, worker) = start(worker_id, stream, config, barrier)?;
    worker.await.map_err(NacelleError::protocol)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::net::TcpListener;
    use std::time::Duration;

    fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
        let client = TcpStream::connect(listener.local_addr().expect("address")).expect("connect");
        let (peer, _) = listener.accept().expect("accept");
        peer.set_read_timeout(Some(Duration::from_secs(1)))
            .expect("timeout");
        (client, peer)
    }

    #[tokio::test]
    async fn cancellation_unblocks_socket_reads_and_phase_barriers() {
        for reading in [false, true] {
            let (client, mut peer) = pair();
            let barrier = Arc::new(Barrier::new(2));
            let config = StressConfig {
                pipeline: 1,
                duration: Duration::from_secs(60),
                ..StressConfig::default()
            };
            let (guard, worker) = start(0, client, config, barrier.clone()).expect("worker");
            barrier.wait().await;
            if reading {
                barrier.wait().await;
                barrier.wait().await;
                peer.read_exact(&mut [0; 280])
                    .expect("request before blocked read");
            }
            drop(guard);
            let result = tokio::time::timeout(Duration::from_secs(1), worker)
                .await
                .expect("cancelled worker exits")
                .expect("join");
            result.expect_err("cancelled worker should fail");
        }
    }

    struct Fragmented {
        socket: TcpStream,
        bytes: io::Cursor<Vec<u8>>,
    }

    impl Read for Fragmented {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            assert!(
                buffer.len() <= 4096,
                "reader initialized too much buffer space"
            );
            let limit = buffer.len().min(3);
            self.bytes.read(&mut buffer[..limit])
        }
    }

    impl Write for Fragmented {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.socket.write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.socket.flush()
        }
    }

    impl Transport for Fragmented {
        fn socket(&self) -> &TcpStream {
            &self.socket
        }

        fn shutdown_write(&mut self) -> io::Result<()> {
            self.socket.shutdown(Shutdown::Write)
        }
    }

    #[tokio::test]
    async fn adapter_preserves_fragmented_multipart_response_decoding_and_read_bounds() {
        let (socket, _peer) = pair();
        let mut frames = Vec::new();
        for flags in [0_u32, 0b0010] {
            frames.extend(21_u32.to_le_bytes());
            frames.extend(7_u64.to_le_bytes());
            frames.extend(super::super::STRESS_OPCODE.to_le_bytes());
            frames.extend(flags.to_le_bytes());
            frames.push(9);
        }
        let mut reader = BlockingIo(Fragmented {
            socket,
            bytes: io::Cursor::new(frames),
        });
        let mut buffer = BytesMut::with_capacity(64 * 1024);
        let response = nacelle_stress_common::read_response(&mut reader, &mut buffer)
            .await
            .expect("complete response");
        assert_eq!(response.request_id, 7);
        assert_eq!(response.wire_bytes, 50);
    }
}
