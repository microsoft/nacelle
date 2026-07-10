use std::net::SocketAddr;

use bytes::{Buf, BytesMut};
use nacelle::NacelleError;
use nacelle_reference_protocol::{FRAME_FLAG_END, FRAME_FLAG_ERROR};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpSocket;

pub const STRESS_OPCODE: u64 = 1;
pub const SOCKET_BUFFER_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Debug)]
pub struct ResponseSummary {
    pub request_id: u64,
    pub wire_bytes: usize,
}

/// Enables the Windows loopback fast-path (SIO_LOOPBACK_FAST_PATH) on a socket.
///
/// This ioctl bypasses most of the Windows TCP/IP stack for loopback
/// connections. It must be called before bind/connect; accepted sockets inherit
/// the setting from the listening socket.
#[cfg(windows)]
pub fn set_loopback_fast_path(raw: std::os::windows::io::RawSocket) {
    unsafe extern "system" {
        fn WSAIoctl(
            s: usize,
            dw_io_control_code: u32,
            lp_vb_in_buffer: *const std::ffi::c_void,
            cb_in_buffer: u32,
            lp_vb_out_buffer: *mut std::ffi::c_void,
            cb_out_buffer: u32,
            lpcb_bytes_returned: *mut u32,
            lp_overlapped: *mut std::ffi::c_void,
            lp_completion_routine: Option<unsafe extern "system" fn()>,
        ) -> i32;
    }

    const SIO_LOOPBACK_FAST_PATH: u32 = 0x9800_0010;
    let enable: u32 = 1;
    let mut bytes_returned: u32 = 0;

    // SAFETY: `raw` is a valid SOCKET from AsRawSocket; WSAIoctl is thread-safe.
    let _ = unsafe {
        WSAIoctl(
            raw as usize,
            SIO_LOOPBACK_FAST_PATH,
            (&enable) as *const u32 as *const std::ffi::c_void,
            std::mem::size_of::<u32>() as u32,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
            None,
        )
    };
}

/// Creates a TCP socket with platform-specific low-latency options applied.
pub fn make_tcp_socket(addr: &SocketAddr) -> Result<TcpSocket, std::io::Error> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_recv_buffer_size(SOCKET_BUFFER_BYTES)?;
    socket.set_send_buffer_size(SOCKET_BUFFER_BYTES)?;
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawSocket;
        set_loopback_fast_path(socket.as_raw_socket());
    }
    Ok(socket)
}

pub async fn write_request_frame<W>(
    writer: &mut W,
    request_id: u64,
    opcode: u64,
    frame: &mut [u8],
) -> Result<usize, NacelleError>
where
    W: AsyncWrite + Unpin,
{
    let frame_len = frame.len() - 4;
    let frame_len = u32::try_from(frame_len).map_err(|_| NacelleError::FrameTooLarge {
        len: frame_len,
        max: u32::MAX as usize,
    })?;

    frame[0..4].copy_from_slice(&frame_len.to_le_bytes());
    frame[4..12].copy_from_slice(&request_id.to_le_bytes());
    frame[12..20].copy_from_slice(&opcode.to_le_bytes());
    frame[20..24].copy_from_slice(&0_u32.to_le_bytes());

    writer.write_all(frame).await?;
    Ok(frame.len())
}

pub async fn read_response<R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
) -> Result<ResponseSummary, NacelleError>
where
    R: AsyncRead + Unpin,
{
    let mut partial_request_id = None;
    let mut partial_wire_bytes = 0;
    loop {
        let (frame_request_id, flags, frame_wire_bytes) = read_frame(reader, read_buf).await?;
        if flags & FRAME_FLAG_ERROR != 0 {
            return Err(NacelleError::InvalidFrame(
                "stress target returned an error frame",
            ));
        }

        match partial_request_id {
            Some(request_id) if request_id != frame_request_id => {
                return Err(NacelleError::InvalidFrame(
                    "interleaved response frames are not supported by the stress harness",
                ));
            }
            None => partial_request_id = Some(frame_request_id),
            _ => {}
        }

        partial_wire_bytes += frame_wire_bytes;
        if flags & FRAME_FLAG_END != 0 {
            return Ok(ResponseSummary {
                request_id: frame_request_id,
                wire_bytes: partial_wire_bytes,
            });
        }
    }
}

async fn read_frame<R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
) -> Result<(u64, u32, usize), NacelleError>
where
    R: AsyncRead + Unpin,
{
    loop {
        if read_buf.len() >= 4 {
            let frame_len =
                u32::from_le_bytes(read_buf[0..4].try_into().expect("fixed width")) as usize;
            if frame_len < 20 {
                return Err(NacelleError::InvalidFrame(
                    "stress client received a truncated frame",
                ));
            }

            let wire_len = 4 + frame_len;
            if read_buf.len() >= wire_len {
                let request_id =
                    u64::from_le_bytes(read_buf[4..12].try_into().expect("fixed width"));
                let _opcode = u64::from_le_bytes(read_buf[12..20].try_into().expect("fixed width"));
                let flags = u32::from_le_bytes(read_buf[20..24].try_into().expect("fixed width"));
                read_buf.advance(wire_len);
                return Ok((request_id, flags, wire_len));
            }
        }

        if read_buf.capacity() - read_buf.len() < 4096 {
            read_buf.reserve(64 * 1024);
        }
        let bytes_read = reader.read_buf(read_buf).await?;
        if bytes_read == 0 {
            return Err(NacelleError::UnexpectedEof);
        }
    }
}
