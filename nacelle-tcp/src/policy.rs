//! Opt-in, statically dispatched TCP lifecycle admission policies.

use std::future::Future;

use bytes::Bytes;
use nacelle_core::error::NacelleError;
use nacelle_core::pipeline::ConnectionInfo;
use nacelle_core::request::{NacelleBody, NacelleConnectionMeta};
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver};

use crate::protocol::{FrameBuffer, Protocol, TcpCompletion};

/// Application-owned admission at TCP lifecycle boundaries.
///
/// Return an error to reject, or await application capacity before returning a
/// permit. Permits are retained through their connection or request lifetime
/// and released on success, error, or cancellation. Return `None` when no
/// resource needs to be held.
///
/// Hooks run inside the connection task, never in the listener's accept loop.
/// Applications must bound their waits; listener shutdown aborts outstanding
/// connection tasks after its drain deadline. Existing framework limits remain
/// active during the staged policy migration.
pub trait TcpLifecyclePolicy<Request, State>: Send + Sync + 'static {
    /// Owned lease held until the corresponding connection or request ends.
    type Permit: Send + 'static;

    /// Admit an accepted socket before TLS detection or handshake.
    ///
    /// Only listener-owned sockets visit this checkpoint. Direct `serve_io`
    /// callers own acceptance and start at [`Self::transport_ready`].
    fn accepted(
        &self,
        _connection: &NacelleConnectionMeta,
    ) -> impl Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        async { Ok(None) }
    }

    /// Admit a ready transport before connection buffers or protocol state.
    ///
    /// TLS metadata is available after a successful handshake. Plaintext and
    /// Unix transports visit this checkpoint without TLS metadata.
    fn transport_ready(
        &self,
        _connection: &NacelleConnectionMeta,
    ) -> impl Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        async { Ok(None) }
    }

    /// Admit the next head read/decode, including already buffered heads.
    ///
    /// This may run before an idle wait or clean EOF: a request does not exist
    /// yet. Its permit is held through response delivery, or until EOF/error.
    fn before_request(
        &self,
        _connection: &ConnectionInfo,
        _state: &State,
    ) -> impl Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        async { Ok(None) }
    }

    /// Admit a decoded head before body-specific allocation or further reads.
    ///
    /// Decoder read-ahead may already contain body bytes. This hook alone does
    /// not provide demand-driven or strict header-only transport reads.
    fn request_head(
        &self,
        _request: &Request,
        _body_len: usize,
        _connection: &ConnectionInfo,
        _state: &State,
    ) -> impl Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        async { Ok(None) }
    }

    /// Observe/admit completion of successful body input.
    ///
    /// For buffered bodies this precedes the handler. For streaming bodies the
    /// handler may already have processed chunks. This is not whole-body
    /// application validation and does not imply that chunks were consumed.
    fn body_complete(
        &self,
        _connection: &NacelleConnectionMeta,
        _body_len: usize,
    ) -> impl Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        async { Ok(None) }
    }
}

impl<Request, State> TcpLifecyclePolicy<Request, State> for () {
    type Permit = ();
}

/// A protocol paired with an application-owned lifecycle policy.
///
/// Construct with [`Protocol::with_policy`]. The wire protocol, decoder, body
/// modes, and response/error encoding are delegated unchanged. Wrapping an
/// already wrapped protocol replaces its lifecycle policy.
pub struct WithTcpPolicy<P, Policy> {
    protocol: P,
    policy: Policy,
}

impl<P, Policy> WithTcpPolicy<P, Policy> {
    pub(crate) const fn new(protocol: P, policy: Policy) -> Self {
        Self { protocol, policy }
    }
}

impl<P, Policy> Protocol for WithTcpPolicy<P, Policy>
where
    P: Protocol,
    Policy: TcpLifecyclePolicy<P::Request, P::ConnectionState>,
{
    type Request = P::Request;
    type Response = P::Response;
    type ConnectionState = P::ConnectionState;
    type Decoder = P::Decoder;
    type ResponseContext = P::ResponseContext;
    type ErrorContext = P::ErrorContext;

    fn policy(&self) -> &impl TcpLifecyclePolicy<Self::Request, Self::ConnectionState> {
        &self.policy
    }

    fn name(&self) -> &'static str {
        self.protocol.name()
    }

    fn decoder(&self, max_frame_len: usize) -> Self::Decoder {
        self.protocol.decoder(max_frame_len)
    }

    fn connection_state(&self, connection: &ConnectionInfo) -> Self::ConnectionState {
        self.protocol.connection_state(connection)
    }

    fn request_wire_bytes(&self, request: &Self::Request, body_len: usize) -> usize {
        self.protocol.request_wire_bytes(request, body_len)
    }

    fn max_request_body_bytes(
        &self,
        request: &Self::Request,
        connection: &ConnectionInfo,
        state: &Self::ConnectionState,
        default_limit: usize,
    ) -> usize {
        self.protocol
            .max_request_body_bytes(request, connection, state, default_limit)
    }

    fn response_context(&self, request: &Self::Request) -> Self::ResponseContext {
        self.protocol.response_context(request)
    }

    fn error_context(&self, request: &Self::Request) -> Self::ErrorContext {
        self.protocol.error_context(request)
    }

    fn apply_response(&self, context: &mut Self::ResponseContext, response: &Self::Response) {
        self.protocol.apply_response(context, response);
    }

    fn max_response_frame_overhead(&self) -> usize {
        self.protocol.max_response_frame_overhead()
    }

    fn response_body(&self, response: Self::Response) -> NacelleBody {
        self.protocol.response_body(response)
    }

    fn encode_response_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.protocol.encode_response_chunk(context, chunk, dst)
    }

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.protocol
            .encode_response_terminal_chunk(context, chunk, dst)
    }

    fn encode_response_end(
        &self,
        context: &mut Self::ResponseContext,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.protocol.encode_response_end(context, dst)
    }

    fn error_response(
        &self,
        context: Option<&Self::ErrorContext>,
        error: &NacelleError,
    ) -> Option<TcpCompletion<Self::Response, Self::ResponseContext>> {
        self.protocol.error_response(context, error)
    }
}

pub(crate) async fn accept<P: Protocol, Observer: NacelleTelemetryObserver>(
    protocol: &P,
    connection: &NacelleConnectionMeta,
    telemetry: &NacelleTelemetry<Observer>,
) -> Result<impl Send + 'static, NacelleError> {
    protocol
        .policy()
        .accepted(connection)
        .await
        .map_err(|error| {
            crate::policies::reject_connection(
                protocol,
                connection,
                telemetry,
                "policy_accepted",
                error,
            )
        })
}
