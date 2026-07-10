use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use nacelle::prelude::*;
use nacelle_reference_protocol::{
    FrameErrorContext, FrameRequest, FrameResponseContext, LengthDelimitedProtocol,
    LengthDelimitedRequestDecoder,
};

#[derive(Debug)]
struct AppXCore {
    prefix: &'static [u8],
}

impl AppXCore {
    async fn handle(&self, mut request: NacelleRequest) -> Result<NacelleResponse, NacelleError> {
        let mut body = BytesMut::new();
        body.extend_from_slice(self.prefix);
        while let Some(chunk) = request.body.next_chunk().await {
            body.extend_from_slice(&chunk?);
        }

        Ok(NacelleResponse::tcp_bytes(body.freeze()))
    }
}

#[derive(Debug, Clone)]
struct AppXProtocol {
    name: &'static str,
    inner: LengthDelimitedProtocol,
}

impl AppXProtocol {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            inner: LengthDelimitedProtocol,
        }
    }
}

impl Protocol<FrameRequest> for AppXProtocol {
    type Decoder = LengthDelimitedRequestDecoder;
    type ResponseContext = FrameResponseContext;
    type ErrorContext = FrameErrorContext;

    fn name(&self) -> &'static str {
        self.name
    }

    fn decoder(&self, max_frame_len: usize) -> Self::Decoder {
        self.inner.decoder(max_frame_len)
    }

    fn response_context(&self, req: &FrameRequest) -> Self::ResponseContext {
        self.inner.response_context(req)
    }

    fn error_context(&self, req: &FrameRequest) -> Self::ErrorContext {
        self.inner.error_context(req)
    }

    fn apply_tcp_response_meta(&self, context: &mut Self::ResponseContext, meta: &TcpResponseMeta) {
        self.inner.apply_tcp_response_meta(context, meta);
    }

    fn encode_response_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError> {
        self.inner.encode_response_chunk(context, chunk, dst)
    }

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError> {
        self.inner
            .encode_response_terminal_chunk(context, chunk, dst)
    }

    fn encode_response_end(
        &self,
        context: &mut Self::ResponseContext,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError> {
        self.inner.encode_response_end(context, dst)
    }

    fn encode_error(
        &self,
        context: Option<&Self::ErrorContext>,
        error: &NacelleError,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError> {
        self.inner.encode_error(context, error, dst)
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let v1_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;
    let v2_addr = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:8081".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let core = Arc::new(AppXCore { prefix: b"appx:" });
    let handler = handler_fn({
        let core = core.clone();
        move |request| {
            let core = core.clone();
            async move { core.handle(request).await }
        }
    });

    let protocols = NacelleProtocols::new()
        .tcp::<FrameRequest, _>("appx-v1", v1_addr, AppXProtocol::new("appx-v1"))
        .tcp::<FrameRequest, _>("appx-v2", v2_addr, AppXProtocol::new("appx-v2"));

    println!("AppX v1 listening on {v1_addr}");
    println!("AppX v2 listening on {v2_addr}");

    NacelleApp::new(handler)
        .with_ctrl_c_shutdown()
        .serve(protocols)
        .await
}
