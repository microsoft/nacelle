use super::*;
use crate::limits::NacelleTcpLimits;
use crate::protocol::TcpHandlerCompletion;

mod admission;
mod tracking;

#[derive(Clone, Default)]
struct CustomProtocol {
    errors: Arc<std::sync::Mutex<Vec<Option<u8>>>>,
    encodes: Arc<AtomicUsize>,
    error_outcomes: Option<Arc<std::sync::Mutex<Vec<CapturedOutcome>>>>,
    silent_errors: bool,
    fail_encoding: bool,
}

struct CustomDecoder;

impl MessageDecoder for CustomDecoder {
    type Message = DecodedRequest<u8>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.first() == Some(&u8::MAX) {
            return Err(NacelleError::InvalidFrame("custom malformed head"));
        }
        if src.len() < 2 {
            return Ok(None);
        }
        let head = src.split_to(2);
        Ok(Some(DecodedRequest {
            request: head[0],
            body_len: usize::from(head[1]),
        }))
    }
}

struct CustomResponse {
    tag: u8,
    body: NacelleBody,
}

impl Protocol for CustomProtocol {
    type Request = u8;
    type Response = CustomResponse;
    type ConnectionState = ();
    type Decoder = CustomDecoder;
    type ResponseContext = u8;
    type ErrorContext = u8;

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        CustomDecoder
    }

    fn connection_state(&self, _connection: &ConnectionInfo) -> Self::ConnectionState {}

    fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
        2 + body_len
    }

    fn response_context(&self, request: &Self::Request) -> Self::ResponseContext {
        *request
    }

    fn error_context(&self, request: &Self::Request) -> Self::ErrorContext {
        *request
    }

    fn apply_response(&self, context: &mut Self::ResponseContext, response: &Self::Response) {
        *context = response.tag;
    }

    fn max_response_frame_overhead(&self) -> usize {
        3
    }

    fn response_body(&self, response: Self::Response) -> NacelleBody {
        response.body
    }

    fn encode_response_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encodes.fetch_add(1, Ordering::SeqCst);
        dst.extend_from_slice(&[b'[', *context])?;
        dst.extend_from_slice(&chunk)?;
        if self.fail_encoding {
            return Err(NacelleError::InvalidFrame("custom response encoder"));
        }
        dst.extend_from_slice(b"]")
    }

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, chunk, dst)
    }

    fn encode_response_end(
        &self,
        context: &mut Self::ResponseContext,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, Bytes::new(), dst)
    }

    fn error_response(
        &self,
        context: Option<&Self::ErrorContext>,
        error: &NacelleError,
    ) -> Option<TcpCompletion<Self::Response, Self::ResponseContext>> {
        self.errors
            .lock()
            .expect("error recorder poisoned")
            .push(context.copied());
        if self.silent_errors {
            return None;
        }
        let reason = match error {
            NacelleError::InvalidFrame(_) => "malformed",
            NacelleError::Handler(_) => "handler",
            NacelleError::Timeout(reason) => reason.as_str(),
            NacelleError::ResourceLimit(reason) => reason.as_str(),
            NacelleError::AdmissionRejected(reason) => reason.as_str(),
            NacelleError::UnexpectedEof => "eof",
            error => panic!("unexpected error hook input: {error}"),
        };
        let completion = TcpCompletion::new(
            CustomResponse {
                tag: context.copied().unwrap_or(b'-'),
                body: NacelleBody::bytes(format!("custom:{reason}")),
            },
            // The ordinary apply_response hook must replace this sentinel.
            u8::MAX,
        );
        Some(match &self.error_outcomes {
            Some(outcomes) => {
                let outcomes = outcomes.clone();
                completion.with_completion_item(move |outcome| {
                    outcomes
                        .lock()
                        .expect("outcome recorder poisoned")
                        .push(capture_outcome(outcome));
                })
            }
            None => completion,
        })
    }
}

fn assert_released(runtime_state: &NacelleRuntimeState) {
    assert_eq!(runtime_state.active_connections(), 0);
    assert_eq!(runtime_state.active_requests(), 0);
    assert_eq!(runtime_state.active_streaming_tasks(), 0);
}

async fn exchange<H>(
    protocol: CustomProtocol,
    handler: H,
    input: &[u8],
    config: NacelleTcpConfig,
    tcp_limits: NacelleTcpLimits,
    runtime_state: NacelleRuntimeState,
) -> (Result<(), NacelleError>, Vec<u8>)
where
    H: crate::admission::TcpService<CustomProtocol> + Send + Sync + 'static,
{
    let (mut client, server_io) = tokio::io::duplex(256);
    let server = TcpServer::<CustomProtocol>::builder()
        .protocol(protocol)
        .handler(handler)
        .tcp_config(config)
        .tcp_limits(tcp_limits)
        .runtime_state(runtime_state)
        .expect("runtime configuration")
        .build()
        .expect("custom server should build");
    let client = async {
        client.write_all(input).await.expect("input should write");
        let mut wire = Vec::new();
        client
            .read_to_end(&mut wire)
            .await
            .expect("error reply should read and connection should close");
        wire
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(server.serve_io(server_io), client)
    })
    .await
    .expect("error must close connection without waiting for more input")
}

fn assert_error_wire(protocol: &CustomProtocol, context: Option<u8>, wire: &[u8], expected: &[u8]) {
    assert_eq!(wire, expected);
    assert_eq!(
        *protocol.errors.lock().expect("error recorder poisoned"),
        vec![context],
    );
    assert_eq!(protocol.encodes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn decoder_error_uses_custom_response_with_no_request_context() {
    let protocol = CustomProtocol::default();
    let runtime_state = NacelleRuntimeState::default();
    let called = Arc::new(AtomicBool::new(false));
    let (result, wire) = exchange(
        protocol.clone(),
        handler_fn({
            let called = called.clone();
            move |context: TcpRequestContext<CustomProtocol>| {
                called.store(true, Ordering::SeqCst);
                async move { Ok(context.complete()) }
            }
        }),
        &[u8::MAX],
        NacelleTcpConfig::default(),
        NacelleTcpLimits::default(),
        runtime_state.clone(),
    )
    .await;

    assert!(matches!(
        result,
        Err(NacelleError::InvalidFrame("custom malformed head"))
    ));
    assert!(!called.load(Ordering::SeqCst));
    assert_error_wire(&protocol, None, &wire, b"[-custom:malformed]");
    assert_released(&runtime_state);
}

#[tokio::test]
async fn handler_error_uses_custom_response_with_request_context() {
    for mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
        let protocol = CustomProtocol::default();
        let runtime_state = NacelleRuntimeState::default();
        let (result, wire) = exchange(
            protocol.clone(),
            handler_fn(|_context: TcpRequestContext<CustomProtocol>| async {
                Err::<TcpHandlerCompletion<CustomProtocol>, _>(NacelleError::handler(
                    "application-only detail",
                ))
            }),
            b"A\0",
            NacelleTcpConfig::default().with_request_body_mode(mode),
            NacelleTcpLimits::default(),
            runtime_state.clone(),
        )
        .await;

        assert!(matches!(result, Err(NacelleError::Handler(_))));
        assert_error_wire(&protocol, Some(b'A'), &wire, b"[Acustom:handler]");
        assert_released(&runtime_state);
    }
}

#[tokio::test]
async fn framework_error_completion_item_reports_exact_wire_bytes_once() {
    for policy in [
        ResponseWritePolicy::Immediate,
        ResponseWritePolicy::CoalesceBuffered,
        ResponseWritePolicy::FlushAtBytes(64),
    ] {
        for (input, context, expected) in [
            (&b"\xff"[..], None, &b"[-custom:malformed]"[..]),
            (&b"A\0"[..], Some(b'A'), &b"[Acustom:handler]"[..]),
        ] {
            let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
            let protocol = CustomProtocol {
                error_outcomes: Some(outcomes.clone()),
                ..CustomProtocol::default()
            };
            let runtime_state = NacelleRuntimeState::default();
            let (result, wire) = exchange(
                protocol.clone(),
                handler_fn(|_context: TcpRequestContext<CustomProtocol>| async {
                    Err::<TcpHandlerCompletion<CustomProtocol>, _>(NacelleError::handler("failure"))
                }),
                input,
                NacelleTcpConfig::default().with_response_write_policy(policy),
                NacelleTcpLimits::default(),
                runtime_state.clone(),
            )
            .await;

            match context {
                Some(_) => assert!(matches!(result, Err(NacelleError::Handler(_)))),
                None => assert!(matches!(
                    result,
                    Err(NacelleError::InvalidFrame("custom malformed head"))
                )),
            }
            assert_error_wire(&protocol, context, &wire, expected);
            assert_eq!(
                *outcomes.lock().expect("outcome recorder poisoned"),
                vec![CapturedOutcome::Delivered {
                    encoded_wire_bytes: expected.len(),
                    written_wire_bytes: expected.len(),
                }],
            );
            assert_released(&runtime_state);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn handler_timeout_encodes_custom_response_without_waiting_for_body() {
    for mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
        let protocol = CustomProtocol::default();
        let runtime_state = NacelleRuntimeState::new(
            NacelleLimits::default().with_handler_timeout(Duration::from_millis(20)),
        );
        let input: &[u8] = match mode {
            TcpRequestBodyMode::Buffered => b"A\0",
            TcpRequestBodyMode::Streaming => b"A\x02x",
        };
        let (result, wire) = exchange(
            protocol.clone(),
            handler_fn(|_context: TcpRequestContext<CustomProtocol>| async {
                std::future::pending::<Result<TcpHandlerCompletion<CustomProtocol>, NacelleError>>()
                    .await
            }),
            input,
            NacelleTcpConfig::default().with_request_body_mode(mode),
            NacelleTcpLimits::default(),
            runtime_state.clone(),
        )
        .await;

        assert!(matches!(
            result,
            Err(NacelleError::Timeout(NacelleTimeoutReason::Handler))
        ));
        assert_error_wire(&protocol, Some(b'A'), &wire, b"[Acustom:handler]");
        assert_released(&runtime_state);
    }
}

#[tokio::test]
async fn body_limit_encodes_custom_response_before_reading_body_or_dispatching() {
    let protocol = CustomProtocol::default();
    let runtime_state =
        NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(1));
    let called = Arc::new(AtomicBool::new(false));
    let (result, wire) = exchange(
        protocol.clone(),
        handler_fn({
            let called = called.clone();
            move |context: TcpRequestContext<CustomProtocol>| {
                called.store(true, Ordering::SeqCst);
                async move { Ok(context.complete()) }
            }
        }),
        b"A\x02",
        NacelleTcpConfig::default(),
        NacelleTcpLimits::default(),
        runtime_state.clone(),
    )
    .await;

    assert!(matches!(
        result,
        Err(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::RequestBodyBytes
        ))
    ));
    assert!(!called.load(Ordering::SeqCst));
    assert_error_wire(
        &protocol,
        Some(b'A'),
        &wire,
        b"[Acustom:request_body_bytes]",
    );
    assert_released(&runtime_state);
}

#[tokio::test]
async fn request_admission_rejection_encodes_custom_response_and_releases_connection() {
    let protocol = CustomProtocol::default();
    let runtime_state =
        NacelleRuntimeState::new(NacelleLimits::default().with_max_in_flight_requests(1));
    let occupied = runtime_state
        .acquire_request()
        .expect("test should occupy request capacity");
    let called = Arc::new(AtomicBool::new(false));
    let (result, wire) = exchange(
        protocol.clone(),
        handler_fn({
            let called = called.clone();
            move |context: TcpRequestContext<CustomProtocol>| {
                called.store(true, Ordering::SeqCst);
                async move { Ok(context.complete()) }
            }
        }),
        b"A\0",
        NacelleTcpConfig::default(),
        NacelleTcpLimits::default(),
        runtime_state.clone(),
    )
    .await;

    assert!(matches!(
        result,
        Err(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::InFlightRequests
        ))
    ));
    assert!(!called.load(Ordering::SeqCst));
    assert_error_wire(
        &protocol,
        Some(b'A'),
        &wire,
        b"[Acustom:in_flight_requests]",
    );
    assert_eq!(runtime_state.active_requests(), 1);
    drop(occupied);
    assert_released(&runtime_state);
    drop(
        runtime_state
            .acquire_request()
            .expect("request capacity should recover"),
    );
}

#[tokio::test(start_paused = true)]
async fn idle_and_active_head_timeouts_have_no_request_context() {
    for (input, reason, expected) in [
        (&b""[..], NacelleTimeoutReason::Idle, &b"[-custom:idle]"[..]),
        (
            &b"A"[..],
            NacelleTimeoutReason::TcpRead,
            &b"[-custom:tcp_read]"[..],
        ),
    ] {
        let protocol = CustomProtocol::default();
        let runtime_state = NacelleRuntimeState::default();
        let called = Arc::new(AtomicBool::new(false));
        let (result, wire) = exchange(
            protocol.clone(),
            handler_fn({
                let called = called.clone();
                move |context: TcpRequestContext<CustomProtocol>| {
                    called.store(true, Ordering::SeqCst);
                    async move { Ok(context.complete()) }
                }
            }),
            input,
            NacelleTcpConfig::default(),
            NacelleTcpLimits::default()
                .with_idle_timeout(Duration::from_millis(20))
                .with_read_timeout(Duration::from_millis(20)),
            runtime_state.clone(),
        )
        .await;

        assert!(matches!(result, Err(NacelleError::Timeout(actual)) if actual == reason));
        assert!(!called.load(Ordering::SeqCst));
        assert_error_wire(&protocol, None, &wire, expected);
        assert_released(&runtime_state);
    }
}

#[tokio::test(start_paused = true)]
async fn body_read_timeout_uses_known_request_context_in_both_body_modes() {
    for mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
        let protocol = CustomProtocol::default();
        let runtime_state = NacelleRuntimeState::default();
        let (result, wire) = exchange(
            protocol.clone(),
            handler_fn(|_context: TcpRequestContext<CustomProtocol>| async {
                std::future::pending::<Result<TcpHandlerCompletion<CustomProtocol>, NacelleError>>()
                    .await
            }),
            b"A\x02x",
            NacelleTcpConfig::default().with_request_body_mode(mode),
            NacelleTcpLimits::default().with_read_timeout(Duration::from_millis(20)),
            runtime_state.clone(),
        )
        .await;

        assert!(matches!(
            result,
            Err(NacelleError::Timeout(NacelleTimeoutReason::RequestBodyRead))
        ));
        assert_error_wire(&protocol, Some(b'A'), &wire, b"[Acustom:request_body_read]");
        assert_released(&runtime_state);
    }
}

#[tokio::test]
async fn explicit_none_error_response_closes_without_encoding_any_bytes() {
    for (input, context) in [(&b"\xff"[..], None), (&b"A\0"[..], Some(b'A'))] {
        let protocol = CustomProtocol {
            silent_errors: true,
            ..CustomProtocol::default()
        };
        let runtime_state = NacelleRuntimeState::default();
        let (result, wire) = exchange(
            protocol.clone(),
            handler_fn(|_context: TcpRequestContext<CustomProtocol>| async {
                Err::<TcpHandlerCompletion<CustomProtocol>, _>(NacelleError::handler("failure"))
            }),
            input,
            NacelleTcpConfig::default(),
            NacelleTcpLimits::default(),
            runtime_state.clone(),
        )
        .await;

        assert!(result.is_err());
        assert!(wire.is_empty());
        assert_eq!(protocol.encodes.load(Ordering::SeqCst), 0);
        assert_eq!(
            *protocol.errors.lock().expect("error recorder poisoned"),
            vec![context],
        );
        assert_released(&runtime_state);
    }
}

async fn late_body_failure_after_silent_completion(close_request: bool) {
    let configs =
        [NacelleTcpConfig::default().with_request_body_mode(TcpRequestBodyMode::Streaming)];
    for config in configs {
        let protocol = CustomProtocol::default();
        let runtime_state = NacelleRuntimeState::default();
        let completed = Arc::new(tokio::sync::Notify::new());
        let (mut client, server_io) = tokio::io::duplex(256);
        let server = TcpServer::<CustomProtocol>::builder()
            .protocol(protocol.clone())
            .handler(handler_fn({
                let completed = completed.clone();
                move |context: TcpRequestContext<CustomProtocol>| {
                    let completed = completed.clone();
                    async move {
                        let completion = context.complete();
                        completed.notify_one();
                        Ok(completion)
                    }
                }
            }))
            .tcp_config(config)
            .tcp_limits(NacelleTcpLimits::default().with_read_timeout(Duration::from_millis(20)))
            .runtime_state(runtime_state.clone())
            .expect("runtime configuration")
            .build()
            .expect("custom server should build");
        let client = async {
            client
                .write_all(b"A\x02x")
                .await
                .expect("partial body should write");
            completed.notified().await;
            assert_eq!(protocol.encodes.load(Ordering::SeqCst), 0);
            assert_eq!(runtime_state.active_requests(), 1);
            assert_eq!(runtime_state.active_streaming_tasks(), 1);
            if close_request {
                client.shutdown().await.expect("request side should close");
            }
            let mut wire = Vec::new();
            client
                .read_to_end(&mut wire)
                .await
                .expect("late body error should read");
            wire
        };

        let (result, wire) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(server.serve_io(server_io), client)
        })
        .await
        .expect("late body failure should close the connection");

        if close_request {
            assert!(matches!(result, Err(NacelleError::UnexpectedEof)));
            assert_error_wire(&protocol, Some(b'A'), &wire, b"[Acustom:eof]");
        } else {
            assert!(matches!(
                result,
                Err(NacelleError::Timeout(NacelleTimeoutReason::RequestBodyRead))
            ));
            assert_error_wire(&protocol, Some(b'A'), &wire, b"[Acustom:request_body_read]");
        }
        assert_released(&runtime_state);
    }
}

#[tokio::test(start_paused = true)]
async fn late_body_eof_after_silent_completion_still_encodes_framework_error() {
    late_body_failure_after_silent_completion(true).await;
}

#[tokio::test(start_paused = true)]
async fn late_body_timeout_after_silent_completion_still_encodes_framework_error() {
    late_body_failure_after_silent_completion(false).await;
}

#[tokio::test]
async fn streaming_admission_failure_encodes_error_before_handler_dispatch() {
    let protocol = CustomProtocol::default();
    let runtime_state =
        NacelleRuntimeState::new(NacelleLimits::default().with_max_streaming_tasks(1));
    let occupied = runtime_state
        .acquire_streaming_task()
        .expect("test should occupy streaming capacity");
    let called = Arc::new(AtomicBool::new(false));
    let (result, wire) = exchange(
        protocol.clone(),
        handler_fn({
            let called = called.clone();
            move |context: TcpRequestContext<CustomProtocol>| {
                called.store(true, Ordering::SeqCst);
                async move { Ok(context.complete()) }
            }
        }),
        b"A\x02",
        NacelleTcpConfig::default().with_request_body_mode(TcpRequestBodyMode::Streaming),
        NacelleTcpLimits::default(),
        runtime_state.clone(),
    )
    .await;

    assert!(matches!(
        result,
        Err(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::StreamingTasks
        ))
    ));
    assert!(!called.load(Ordering::SeqCst));
    assert_error_wire(&protocol, Some(b'A'), &wire, b"[Acustom:streaming_tasks]");
    assert_eq!(runtime_state.active_streaming_tasks(), 1);
    drop(occupied);
    assert_released(&runtime_state);
    drop(
        runtime_state
            .acquire_streaming_task()
            .expect("streaming capacity should recover"),
    );
}

#[tokio::test]
async fn error_reply_encoding_failure_is_not_retried_and_partial_frame_is_discarded() {
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let protocol = CustomProtocol {
        fail_encoding: true,
        error_outcomes: Some(outcomes.clone()),
        ..CustomProtocol::default()
    };
    let runtime_state = NacelleRuntimeState::default();
    let (result, wire) = exchange(
        protocol.clone(),
        handler_fn(|_context: TcpRequestContext<CustomProtocol>| async {
            Err::<TcpHandlerCompletion<CustomProtocol>, _>(NacelleError::handler("failure"))
        }),
        b"A\0",
        NacelleTcpConfig::default(),
        NacelleTcpLimits::default(),
        runtime_state.clone(),
    )
    .await;

    assert!(matches!(
        result,
        Err(NacelleError::InvalidFrame("custom response encoder"))
    ));
    assert!(wire.is_empty());
    assert_eq!(
        *protocol.errors.lock().expect("error recorder poisoned"),
        vec![Some(b'A')],
    );
    assert_eq!(protocol.encodes.load(Ordering::SeqCst), 1);
    assert_eq!(
        *outcomes.lock().expect("outcome recorder poisoned"),
        vec![CapturedOutcome::Failed {
            phase: ResponseDeliveryPhase::Encode,
            encoded_wire_bytes: 0,
            written_wire_bytes: 0,
        }],
    );
    assert_released(&runtime_state);
}

#[tokio::test]
async fn error_reply_write_failure_does_not_reenter_error_hook() {
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let protocol = CustomProtocol {
        error_outcomes: Some(outcomes.clone()),
        ..CustomProtocol::default()
    };
    let write_attempts = Arc::new(AtomicUsize::new(0));
    let shutdown_attempts = Arc::new(AtomicUsize::new(0));
    let runtime_state = NacelleRuntimeState::default();
    let server = TcpServer::<CustomProtocol>::builder()
        .protocol(protocol.clone())
        .handler(handler_fn(
            |context: TcpRequestContext<CustomProtocol>| async move { Ok(context.complete()) },
        ))
        .runtime_state(runtime_state.clone())
        .expect("runtime configuration")
        .build()
        .expect("custom server should build");
    let result = server
        .serve_io(FailingErrorWriteIo {
            input_read: false,
            write_attempts: write_attempts.clone(),
            shutdown_attempts: shutdown_attempts.clone(),
        })
        .await;

    assert!(matches!(
        result,
        Err(NacelleError::Io(ref error)) if error.kind() == std::io::ErrorKind::BrokenPipe
    ));
    assert_eq!(write_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(shutdown_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        *protocol.errors.lock().expect("error recorder poisoned"),
        vec![None],
    );
    assert_eq!(protocol.encodes.load(Ordering::SeqCst), 1);
    assert_eq!(
        *outcomes.lock().expect("outcome recorder poisoned"),
        vec![CapturedOutcome::Failed {
            phase: ResponseDeliveryPhase::SocketWrite,
            encoded_wire_bytes: b"[-custom:malformed]".len(),
            written_wire_bytes: 0,
        }],
    );
    assert_released(&runtime_state);
}

#[tokio::test]
async fn ordinary_response_encoding_failure_does_not_invoke_error_hook() {
    let protocol = CustomProtocol {
        fail_encoding: true,
        ..CustomProtocol::default()
    };
    let runtime_state = NacelleRuntimeState::default();
    let (result, wire) = exchange(
        protocol.clone(),
        handler_fn(|context: TcpRequestContext<CustomProtocol>| async move {
            context
                .respond(CustomResponse {
                    tag: b'A',
                    body: NacelleBody::bytes("ordinary"),
                })
                .await
        }),
        b"A\0",
        NacelleTcpConfig::default(),
        NacelleTcpLimits::default(),
        runtime_state.clone(),
    )
    .await;

    assert!(matches!(
        result,
        Err(NacelleError::InvalidFrame("custom response encoder"))
    ));
    assert!(wire.is_empty());
    assert!(
        protocol
            .errors
            .lock()
            .expect("error recorder poisoned")
            .is_empty()
    );
    assert_eq!(protocol.encodes.load(Ordering::SeqCst), 1);
    assert_released(&runtime_state);
}

#[tokio::test]
async fn ordinary_response_write_failure_does_not_invoke_error_hook() {
    let protocol = CustomProtocol::default();
    let runtime_state = NacelleRuntimeState::default();
    let (mut client, server_io) = tokio::io::duplex(64);
    client
        .write_all(b"A\0")
        .await
        .expect("request should write");
    drop(client);
    let server = TcpServer::<CustomProtocol>::builder()
        .protocol(protocol.clone())
        .handler(handler_fn(
            |context: TcpRequestContext<CustomProtocol>| async move {
                context
                    .respond(CustomResponse {
                        tag: b'A',
                        body: NacelleBody::bytes("ordinary"),
                    })
                    .await
            },
        ))
        .runtime_state(runtime_state.clone())
        .expect("runtime configuration")
        .build()
        .expect("custom server should build");

    let result = server.serve_io(server_io).await;

    assert!(matches!(
        result,
        Err(NacelleError::Io(ref error)) if error.kind() == std::io::ErrorKind::BrokenPipe
    ));
    assert!(
        protocol
            .errors
            .lock()
            .expect("error recorder poisoned")
            .is_empty()
    );
    assert_eq!(protocol.encodes.load(Ordering::SeqCst), 1);
    assert_released(&runtime_state);
}

#[tokio::test]
async fn silent_completion_is_distinct_from_an_encoded_empty_response() {
    for policy in [
        ResponseWritePolicy::Immediate,
        ResponseWritePolicy::CoalesceBuffered,
        ResponseWritePolicy::FlushAtBytes(4),
    ] {
        let protocol = CustomProtocol::default();
        let runtime_state = NacelleRuntimeState::default();
        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dispatched = Arc::new(AtomicUsize::new(0));
        let server = TcpServer::<CustomProtocol>::builder()
            .protocol(protocol.clone())
            .handler(handler_fn({
                let dispatched = dispatched.clone();
                move |context: TcpRequestContext<CustomProtocol>| {
                    dispatched.fetch_add(1, Ordering::SeqCst);
                    async move {
                        match context.request().head {
                            b'S' => Ok(context
                                .complete()
                                .map(|_| panic!("silent completion must not map a response"))),
                            b'E' => {
                                context
                                    .respond(CustomResponse {
                                        tag: b'E',
                                        body: NacelleBody::empty(),
                                    })
                                    .await
                            }
                            head => panic!("unexpected request head: {head}"),
                        }
                    }
                }
            }))
            .tcp_config(NacelleTcpConfig::default().with_response_write_policy(policy))
            .runtime_state(runtime_state.clone())
            .expect("runtime configuration")
            .build()
            .expect("custom server should build");

        server
            .serve_io(RecordingIo {
                input: b"S\0E\0S\0".to_vec(),
                position: 0,
                writes: writes.clone(),
            })
            .await
            .expect("silent and empty responses should share one connection");

        assert_eq!(
            writes.lock().expect("write recorder poisoned").concat(),
            b"[E]"
        );
        assert_eq!(protocol.encodes.load(Ordering::SeqCst), 1);
        assert_eq!(dispatched.load(Ordering::SeqCst), 3);
        assert_released(&runtime_state);
    }
}

struct CustomSerialHandler;

impl SerialTcpHandler<CustomProtocol> for CustomSerialHandler {
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, CustomProtocol>,
    ) -> Result<TcpHandlerCompletion<CustomProtocol>, NacelleError> {
        match context.request().head {
            b'S' => Ok(context.complete()),
            b'E' => {
                context
                    .respond(CustomResponse {
                        tag: b'E',
                        body: NacelleBody::empty(),
                    })
                    .await
            }
            b'X' => Err(NacelleError::handler("serial failure")),
            head => panic!("unexpected request head: {head}"),
        }
    }
}

impl LocalSerialTcpHandler<CustomProtocol> for CustomSerialHandler {
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, CustomProtocol>,
    ) -> Result<TcpHandlerCompletion<CustomProtocol>, NacelleError> {
        SerialTcpHandler::call(self, context).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn serial_and_local_dispatch_preserve_silence_and_encode_custom_errors() {
    for mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
        for local in [false, true] {
            let protocol = CustomProtocol::default();
            let runtime_state = NacelleRuntimeState::default();
            let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
            let io = RecordingIo {
                input: b"S\x02hiE\0S\0X\0".to_vec(),
                position: 0,
                writes: writes.clone(),
            };
            let config = NacelleTcpConfig::default().with_request_body_mode(mode);
            let result = if local {
                LocalSerialTcpServer::new(protocol.clone(), CustomSerialHandler)
                    .with_tcp_config(config)
                    .with_runtime_state(runtime_state.clone())
                    .expect("runtime configuration")
                    .serve_io(io)
                    .await
            } else {
                SerialTcpServer::new(protocol.clone(), CustomSerialHandler)
                    .with_tcp_config(config)
                    .with_runtime_state(runtime_state.clone())
                    .expect("runtime configuration")
                    .serve_io(io)
                    .await
            };

            assert!(matches!(result, Err(NacelleError::Handler(_))));
            assert_eq!(
                writes.lock().expect("write recorder poisoned").concat(),
                b"[E][Xcustom:handler]",
            );
            assert_eq!(protocol.encodes.load(Ordering::SeqCst), 2);
            assert_eq!(
                *protocol.errors.lock().expect("error recorder poisoned"),
                vec![Some(b'X')],
            );
            assert_released(&runtime_state);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn local_handler_silent_completion_drains_body_without_encoding() {
    for mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
        let protocol = CustomProtocol::default();
        let runtime_state = NacelleRuntimeState::default();
        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls = Rc::new(std::cell::Cell::new(0));
        serve_local_stream(
            RecordingIo {
                input: b"S\x02hiS\0".to_vec(),
                position: 0,
                writes: writes.clone(),
            },
            Rc::new(protocol.clone()),
            Rc::new(local_handler_fn({
                let calls = calls.clone();
                move |context: TcpRequestContext<CustomProtocol>| {
                    let calls = calls.clone();
                    async move {
                        tokio::task::yield_now().await;
                        calls.set(calls.get() + 1);
                        assert_eq!(context.request().head, b'S');
                        Ok(context.complete())
                    }
                }
            })),
            NacelleTcpConfig::default().with_request_body_mode(mode),
            NacelleTelemetry::default(),
            runtime_state.clone(),
            NacelleTcpLimits::default(),
            NacelleConnectionMeta::tcp(None, None),
        )
        .await
        .expect("local silent requests should drain cleanly");

        assert_eq!(calls.get(), 2);
        assert!(writes.lock().expect("write recorder poisoned").is_empty());
        assert_eq!(protocol.encodes.load(Ordering::SeqCst), 0);
        assert_released(&runtime_state);
    }
}
