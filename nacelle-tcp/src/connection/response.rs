use bytes::BytesMut;

use crate::protocol::Protocol;
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::response::NacelleResponse;

pub(super) async fn encode_response_body<Req, P>(
    protocol: &P,
    mut context: P::ResponseContext,
    response: NacelleResponse,
    write_buf: &mut BytesMut,
    runtime_state: &NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
{
    let Some(meta) = response.meta.tcp() else {
        return Err(NacelleError::InvalidFrame("non_tcp_response"));
    };
    protocol.apply_tcp_response_meta(&mut context, meta);

    let body = response.body;
    let mut response_body_bytes = 0_usize;
    let mut body = match body.try_into_single_chunk_or_empty() {
        Ok(Some(chunk)) => {
            validate_response_bytes(&mut response_body_bytes, chunk.len(), runtime_state)?;
            protocol.encode_response_terminal_chunk(&mut context, chunk, write_buf)?;
            return Ok(());
        }
        Ok(None) => {
            protocol.encode_response_end(&mut context, write_buf)?;
            return Ok(());
        }
        Err(body) => body,
    };

    let mut pending_chunk = None;
    while let Some(chunk) = body.next_chunk().await {
        let chunk = chunk?;
        if chunk.is_empty() {
            continue;
        }
        validate_response_bytes(&mut response_body_bytes, chunk.len(), runtime_state)?;
        if let Some(prev) = pending_chunk.replace(chunk) {
            protocol.encode_response_chunk(&mut context, prev, write_buf)?;
        }
    }

    if let Some(last) = pending_chunk {
        protocol.encode_response_terminal_chunk(&mut context, last, write_buf)?;
    } else {
        protocol.encode_response_end(&mut context, write_buf)?;
    }

    Ok(())
}

pub(super) fn write_error<Req, P>(
    write_buf: &mut BytesMut,
    protocol: &P,
    context: Option<P::ErrorContext>,
    error: NacelleError,
    buffer_capacity: usize,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
{
    let prev_len = write_buf.len();
    write_buf.reserve(buffer_capacity.max(128));
    if let Err(e) = protocol.encode_error(context.as_ref(), &error, write_buf) {
        write_buf.truncate(prev_len);
        return Err(e);
    }
    Ok(())
}

fn validate_response_bytes(
    total: &mut usize,
    next_chunk_len: usize,
    runtime_state: &NacelleRuntimeState,
) -> Result<(), NacelleError> {
    let Some(next) = total.checked_add(next_chunk_len) else {
        return Err(NacelleError::ResourceLimit("response_body_bytes"));
    };
    if next > runtime_state.limits().max_response_body_bytes {
        return Err(NacelleError::ResourceLimit("response_body_bytes"));
    }
    *total = next;
    Ok(())
}
