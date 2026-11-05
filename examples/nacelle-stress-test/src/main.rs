#[global_allocator]
#[cfg(not(feature = "allocation-counting"))]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[global_allocator]
#[cfg(feature = "allocation-counting")]
static ALLOC: nacelle_stress_common::CountingAllocator<mimalloc::MiMalloc> =
    nacelle_stress_common::CountingAllocator::new(mimalloc::MiMalloc);

use std::collections::VecDeque;
use std::fmt::Debug;
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use nacelle::core::NacelleError;
use nacelle_stress_common::{STRESS_OPCODE, make_tcp_socket, read_response, write_request_frame};
#[cfg(feature = "rustls")]
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
#[cfg(feature = "rustls")]
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
#[cfg(feature = "rustls")]
use rustls::{ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

mod blocking;

const LATENCY_SUB_BUCKET_BITS: u32 = 8;
const LATENCY_SUB_BUCKETS: usize = 1 << LATENCY_SUB_BUCKET_BITS;
const LATENCY_BUCKET_COUNT: usize =
    (u64::BITS - LATENCY_SUB_BUCKET_BITS + 1) as usize * LATENCY_SUB_BUCKETS;

#[derive(Debug, Clone)]
struct StressConfig {
    server_addr: SocketAddr,
    connections: usize,
    pipeline: usize,
    requests_per_connection: Option<usize>,
    duration: Duration,
    warmup: Duration,
    payload_bytes: usize,
    tls_insecure: bool,
    current_thread: bool,
    blocking_io: bool,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            server_addr: SocketAddr::from(([127, 0, 0, 1], 7878)),
            connections: 256,
            pipeline: 8,
            requests_per_connection: None,
            duration: Duration::from_secs(10),
            warmup: Duration::ZERO,
            payload_bytes: 256,
            tls_insecure: false,
            current_thread: false,
            blocking_io: false,
        }
    }
}

#[derive(Debug, Default)]
struct WorkerResult {
    completed_connections: u64,
    warmup_completed_requests: u64,
    completed_requests: u64,
    request_wire_bytes: u64,
    response_wire_bytes: u64,
    latency_total_ns: u128,
    max_latency_ns: u64,
    latency_histogram: LatencyHistogram,
    #[cfg(feature = "phase-timing")]
    preparation_write_flush_ns: u128,
    #[cfg(feature = "phase-timing")]
    response_read_validation_ns: u128,
}

#[derive(Debug)]
struct LatencyHistogram {
    counts: Vec<u64>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            counts: vec![0; LATENCY_BUCKET_COUNT],
        }
    }
}

impl LatencyHistogram {
    fn record(&mut self, latency_ns: u64) {
        let shift = (u64::BITS - 1)
            .saturating_sub(latency_ns.leading_zeros())
            .saturating_sub(LATENCY_SUB_BUCKET_BITS);
        let index = shift as usize * LATENCY_SUB_BUCKETS + (latency_ns >> shift) as usize;
        self.counts[index] += 1;
    }

    // Every completion is recorded. Buckets are exact through 511 ns, then their
    // upper bounds overestimate by less than 1/256 of the recorded latency.
    fn percentile(&self, completed_requests: u64, percentile: u64) -> Option<u64> {
        if completed_requests == 0 {
            return None;
        }
        let rank = (u128::from(completed_requests) * u128::from(percentile)).div_ceil(100);
        let mut cumulative = 0_u128;
        for (index, count) in self.counts.iter().enumerate() {
            cumulative += u128::from(*count);
            if cumulative >= rank {
                let shift = (index / LATENCY_SUB_BUCKETS).saturating_sub(1);
                let significand = index - shift * LATENCY_SUB_BUCKETS;
                let upper_ns = ((significand as u128 + 1) << shift) - 1;
                return Some(upper_ns.min(u128::from(u64::MAX)) as u64);
            }
        }
        None
    }
}

struct WorkerIo {
    request_frame: Vec<u8>,
    read_buf: BytesMut,
    inflight: VecDeque<(u64, Instant)>,
    next_request_id: u64,
}

impl WorkerIo {
    fn new(worker_id: usize, config: &StressConfig) -> Self {
        let payload = vec![worker_id as u8; config.payload_bytes];
        let mut request_frame = vec![0_u8; 24 + payload.len()];
        request_frame[24..].copy_from_slice(&payload);
        Self {
            request_frame,
            read_buf: BytesMut::with_capacity(64 * 1024),
            inflight: VecDeque::with_capacity(config.pipeline),
            next_request_id: (worker_id as u64) << 48,
        }
    }
}

struct SequentialIo {
    request_frame: Vec<u8>,
    read_buf: BytesMut,
    next_request_id: u64,
}

impl SequentialIo {
    fn new(worker_id: usize, config: &StressConfig) -> Self {
        let payload = vec![worker_id as u8; config.payload_bytes];
        let mut request_frame = vec![0_u8; 24 + payload.len()];
        request_frame[24..].copy_from_slice(&payload);
        Self {
            request_frame,
            read_buf: BytesMut::with_capacity(64 * 1024),
            next_request_id: (worker_id as u64) << 48,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args(std::env::args().skip(1))?;
    let mut runtime = if config.current_thread {
        tokio::runtime::Builder::new_current_thread()
    } else {
        tokio::runtime::Builder::new_multi_thread()
    };
    if config.blocking_io {
        // Every worker must reach the phase barriers, including beyond the default pool limit.
        runtime.max_blocking_threads(config.connections);
    }
    runtime.enable_all().build()?.block_on(run(config))
}

async fn run(config: StressConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if config.current_thread {
        println!("stress runtime=current-thread");
    }

    println!(
        "stress target={} connections={} pipeline={} requests_per_connection={} request_payload={}B duration={}s",
        config.server_addr,
        config.connections,
        config.pipeline,
        config
            .requests_per_connection
            .map_or_else(|| "unbounded".to_owned(), |value| value.to_string()),
        config.payload_bytes,
        config.duration.as_secs_f64(),
    );
    if config.tls_insecure {
        println!("stress transport=tcp-tls verify=insecure");
    }
    if config.blocking_io {
        println!(
            "stress io=blocking-sequential worker_threads={}",
            config.connections
        );
    }

    let barrier = Arc::new(Barrier::new(config.connections + 1));
    let mut workers = JoinSet::new();
    for worker_id in 0..config.connections {
        workers.spawn(run_worker(
            worker_id,
            config.server_addr,
            config.clone(),
            barrier.clone(),
        ));
    }

    wait_for_phase(&barrier, &mut workers).await?;
    wait_for_phase(&barrier, &mut workers).await?;
    println!(
        "warmup_completed     elapsed_target={:.6}s",
        config.warmup.as_secs_f64()
    );
    std::io::stdout().flush()?;
    println!("measurement_start");
    std::io::stdout().flush()?;
    #[cfg(feature = "allocation-counting")]
    let allocation_started = nacelle_stress_common::allocation_snapshot();
    let load_started = Instant::now();
    wait_for_phase(&barrier, &mut workers).await?;
    wait_for_phase(&barrier, &mut workers).await?;
    let total_elapsed = load_started.elapsed();
    #[cfg(feature = "allocation-counting")]
    let allocations = nacelle_stress_common::allocation_snapshot().delta_since(allocation_started);
    println!(
        "measurement_end      elapsed={:.6}s",
        total_elapsed.as_secs_f64()
    );
    std::io::stdout().flush()?;
    barrier.wait().await;
    let mut aggregate = WorkerResult::default();
    while let Some(worker) = workers.join_next().await {
        aggregate.merge(worker??);
    }
    print_summary(&config, &aggregate, total_elapsed);
    #[cfg(feature = "phase-timing")]
    print_phase_summary(&aggregate);
    #[cfg(feature = "allocation-counting")]
    print_allocation_summary(allocations, aggregate.completed_requests);
    Ok(())
}

async fn wait_for_phase(
    barrier: &Barrier,
    workers: &mut JoinSet<Result<WorkerResult, NacelleError>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::select! {
        _ = barrier.wait() => Ok(()),
        worker = workers.join_next() => {
            if let Some(worker) = worker {
                let _ = worker??;
            }
            Err("stress worker finished before the measurement boundary".into())
        }
    }
}

async fn run_worker(
    worker_id: usize,
    server_addr: SocketAddr,
    config: StressConfig,
    barrier: Arc<Barrier>,
) -> Result<WorkerResult, NacelleError> {
    let socket = make_tcp_socket(&server_addr)?;
    let connect_started = Instant::now();
    let stream = socket.connect(server_addr).await?;
    let connect_elapsed = connect_started.elapsed();
    stream.set_nodelay(true)?;
    if config.tls_insecure {
        #[cfg(feature = "rustls")]
        {
            let connector = tokio_rustls::TlsConnector::from(Arc::new(insecure_tls_config()));
            let server_name = ServerName::try_from("localhost")
                .map_err(|_| NacelleError::InvalidFrame("invalid TLS server name"))?;
            let handshake_started = Instant::now();
            let stream = connector.connect(server_name, stream).await?;
            let handshake_elapsed = handshake_started.elapsed();
            println!(
                "tls_negotiated connection={} version={:?} cipher={:?} connect_us={:.2} handshake_us={:.2}",
                worker_id,
                stream.get_ref().1.protocol_version(),
                stream
                    .get_ref()
                    .1
                    .negotiated_cipher_suite()
                    .map(|suite| suite.suite()),
                connect_elapsed.as_secs_f64() * 1_000_000.0,
                handshake_elapsed.as_secs_f64() * 1_000_000.0,
            );
            std::io::stdout().flush()?;
            if config.blocking_io {
                let (stream, connection) = stream.into_inner();
                let stream = rustls::StreamOwned::new(connection, blocking::into_std(stream)?);
                return blocking::run(worker_id, stream, config, barrier).await;
            }
            let (mut reader, mut writer) = tokio::io::split(stream);
            let (mut io, mut result) =
                prepare_measurement(worker_id, &mut reader, &mut writer, &config, &barrier).await?;
            let deadline = Instant::now() + config.duration;
            run_worker_io(
                &mut reader,
                &mut writer,
                &mut io,
                &mut result,
                &config,
                deadline,
                usize::MAX,
            )
            .await?;
            barrier.wait().await;
            barrier.wait().await;
            writer.shutdown().await?;
            result.completed_connections = 1;
            return Ok(result);
        }
        #[cfg(not(feature = "rustls"))]
        {
            return Err(NacelleError::InvalidFrame(
                "stress client was built without rustls support",
            ));
        }
    }

    println!(
        "tcp_connected connection={} connect_us={:.2}",
        worker_id,
        connect_elapsed.as_secs_f64() * 1_000_000.0
    );
    std::io::stdout().flush()?;
    if config.blocking_io {
        return blocking::run(worker_id, blocking::into_std(stream)?, config, barrier).await;
    }
    let (mut reader, mut writer) = stream.into_split();
    let (mut io, mut aggregate) =
        prepare_measurement(worker_id, &mut reader, &mut writer, &config, &barrier).await?;
    let deadline = Instant::now() + config.duration;
    loop {
        run_worker_io(
            &mut reader,
            &mut writer,
            &mut io,
            &mut aggregate,
            &config,
            deadline,
            config.requests_per_connection.unwrap_or(usize::MAX),
        )
        .await?;
        aggregate.completed_connections += 1;

        if config.requests_per_connection.is_none() || Instant::now() >= deadline {
            break;
        }
        writer.shutdown().await?;
        let connected = make_tcp_socket(&server_addr)?.connect(server_addr).await?;
        connected.set_nodelay(true)?;
        (reader, writer) = connected.into_split();
        io = WorkerIo::new(worker_id, &config);
    }
    barrier.wait().await;
    barrier.wait().await;
    writer.shutdown().await?;
    Ok(aggregate)
}

async fn run_sequential_worker<S>(
    worker_id: usize,
    mut stream: S,
    config: &StressConfig,
    barrier: &Barrier,
) -> Result<WorkerResult, NacelleError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut io, mut result) =
        prepare_sequential_measurement(worker_id, &mut stream, config, barrier).await?;
    run_sequential_io(
        &mut stream,
        &mut io,
        &mut result,
        Instant::now() + config.duration,
        usize::MAX,
    )
    .await?;
    barrier.wait().await;
    barrier.wait().await;
    stream.shutdown().await?;
    result.completed_connections = 1;
    Ok(result)
}

async fn prepare_sequential_measurement<S>(
    worker_id: usize,
    stream: &mut S,
    config: &StressConfig,
    barrier: &Barrier,
) -> Result<(SequentialIo, WorkerResult), NacelleError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut io = SequentialIo::new(worker_id, config);
    let mut result = WorkerResult::default();
    barrier.wait().await;
    if !config.warmup.is_zero() {
        run_sequential_io(
            stream,
            &mut io,
            &mut result,
            Instant::now() + config.warmup,
            usize::MAX,
        )
        .await?;
        let warmup_requests = result.completed_requests;
        result.reset();
        result.warmup_completed_requests = warmup_requests;
    }
    barrier.wait().await;
    barrier.wait().await;
    Ok((io, result))
}

async fn run_sequential_io<S>(
    stream: &mut S,
    io: &mut SequentialIo,
    result: &mut WorkerResult,
    deadline: Instant,
    request_limit: usize,
) -> Result<(), NacelleError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut sent_requests = 0;
    while sent_requests < request_limit && Instant::now() < deadline {
        let started_at = Instant::now();
        let request_id = io.next_request_id;
        io.next_request_id += 1;
        result.request_wire_bytes +=
            write_request_frame(stream, request_id, STRESS_OPCODE, &mut io.request_frame).await?
                as u64;
        #[cfg(feature = "phase-timing")]
        {
            result.preparation_write_flush_ns += started_at.elapsed().as_nanos();
        }
        #[cfg(feature = "phase-timing")]
        let flush_started = Instant::now();
        stream.flush().await?;
        #[cfg(feature = "phase-timing")]
        {
            result.preparation_write_flush_ns += flush_started.elapsed().as_nanos();
        }

        #[cfg(feature = "phase-timing")]
        let response_started = Instant::now();
        let response = read_response(stream, &mut io.read_buf).await?;
        if response.request_id != request_id {
            return Err(NacelleError::InvalidFrame(
                "response request id was not the next in-flight request",
            ));
        }
        #[cfg(feature = "phase-timing")]
        {
            result.response_read_validation_ns += response_started.elapsed().as_nanos();
        }
        let latency_ns = started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        result.record_completion(latency_ns, response.wire_bytes as u64);
        sent_requests += 1;
    }
    Ok(())
}

async fn prepare_measurement<R, W>(
    worker_id: usize,
    reader: &mut R,
    writer: &mut W,
    config: &StressConfig,
    barrier: &Barrier,
) -> Result<(WorkerIo, WorkerResult), NacelleError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut io = WorkerIo::new(worker_id, config);
    let mut result = WorkerResult::default();
    barrier.wait().await;
    if !config.warmup.is_zero() {
        run_worker_io(
            reader,
            writer,
            &mut io,
            &mut result,
            config,
            Instant::now() + config.warmup,
            usize::MAX,
        )
        .await?;
        let warmup_requests = result.completed_requests;
        result.reset();
        result.warmup_completed_requests = warmup_requests;
    }
    // Warmup drains every in-flight response before the coordinator emits the
    // boundary markers. The next barrier holds all workers until output is flushed.
    barrier.wait().await;
    barrier.wait().await;
    Ok((io, result))
}

async fn run_worker_io<R, W>(
    reader: &mut R,
    writer: &mut W,
    io: &mut WorkerIo,
    result: &mut WorkerResult,
    config: &StressConfig,
    deadline: Instant,
    request_limit: usize,
) -> Result<(), NacelleError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut sent_requests = 0_usize;

    while io.inflight.len() < config.pipeline && sent_requests < request_limit {
        if Instant::now() >= deadline {
            break;
        }
        let started_at = Instant::now();
        let request_id = io.next_request_id;
        io.next_request_id += 1;
        result.request_wire_bytes +=
            write_request_frame(writer, request_id, STRESS_OPCODE, &mut io.request_frame).await?
                as u64;
        #[cfg(feature = "phase-timing")]
        {
            result.preparation_write_flush_ns += started_at.elapsed().as_nanos();
        }
        io.inflight.push_back((request_id, started_at));
        sent_requests += 1;
    }
    if !io.inflight.is_empty() {
        #[cfg(feature = "phase-timing")]
        let flush_started = Instant::now();
        writer.flush().await?;
        #[cfg(feature = "phase-timing")]
        {
            result.preparation_write_flush_ns += flush_started.elapsed().as_nanos();
        }
    }

    while !io.inflight.is_empty() {
        #[cfg(feature = "phase-timing")]
        let response_started = Instant::now();
        let response = read_response(reader, &mut io.read_buf).await?;
        let (expected_request_id, started_at) = io.inflight.pop_front().ok_or(
            NacelleError::InvalidFrame("response completed without an in-flight request"),
        )?;
        if expected_request_id != response.request_id {
            return Err(NacelleError::InvalidFrame(
                "response request id was not the next in-flight request",
            ));
        }
        #[cfg(feature = "phase-timing")]
        {
            result.response_read_validation_ns += response_started.elapsed().as_nanos();
        }

        let latency_ns = started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        result.record_completion(latency_ns, response.wire_bytes as u64);

        if Instant::now() < deadline && sent_requests < request_limit {
            let started_at = Instant::now();
            let request_id = io.next_request_id;
            io.next_request_id += 1;
            result.request_wire_bytes +=
                write_request_frame(writer, request_id, STRESS_OPCODE, &mut io.request_frame)
                    .await? as u64;
            #[cfg(feature = "phase-timing")]
            {
                result.preparation_write_flush_ns += started_at.elapsed().as_nanos();
            }
            io.inflight.push_back((request_id, started_at));
            sent_requests += 1;
            #[cfg(feature = "phase-timing")]
            let flush_started = Instant::now();
            writer.flush().await?;
            #[cfg(feature = "phase-timing")]
            {
                result.preparation_write_flush_ns += flush_started.elapsed().as_nanos();
            }
        }
    }

    Ok(())
}

#[cfg(feature = "rustls")]
fn insecure_tls_config() -> ClientConfig {
    let mut config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(InsecureCertificateVerifier))
        .with_no_client_auth();
    config.alpn_protocols.clear();
    config
}

#[cfg(feature = "rustls")]
#[derive(Debug)]
struct InsecureCertificateVerifier;

#[cfg(feature = "rustls")]
impl ServerCertVerifier for InsecureCertificateVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

impl WorkerResult {
    fn record_completion(&mut self, latency_ns: u64, response_wire_bytes: u64) {
        self.completed_requests += 1;
        self.response_wire_bytes += response_wire_bytes;
        self.latency_total_ns += latency_ns as u128;
        self.max_latency_ns = self.max_latency_ns.max(latency_ns);
        self.latency_histogram.record(latency_ns);
    }

    fn reset(&mut self) {
        self.completed_connections = 0;
        self.warmup_completed_requests = 0;
        self.completed_requests = 0;
        self.request_wire_bytes = 0;
        self.response_wire_bytes = 0;
        self.latency_total_ns = 0;
        self.max_latency_ns = 0;
        self.latency_histogram.counts.fill(0);
        #[cfg(feature = "phase-timing")]
        {
            self.preparation_write_flush_ns = 0;
            self.response_read_validation_ns = 0;
        }
    }

    fn merge(&mut self, other: WorkerResult) {
        self.completed_connections += other.completed_connections;
        self.warmup_completed_requests += other.warmup_completed_requests;
        self.completed_requests += other.completed_requests;
        self.request_wire_bytes += other.request_wire_bytes;
        self.response_wire_bytes += other.response_wire_bytes;
        self.latency_total_ns += other.latency_total_ns;
        self.max_latency_ns = self.max_latency_ns.max(other.max_latency_ns);
        #[cfg(feature = "phase-timing")]
        {
            self.preparation_write_flush_ns += other.preparation_write_flush_ns;
            self.response_read_validation_ns += other.response_read_validation_ns;
        }
        for (count, other_count) in self
            .latency_histogram
            .counts
            .iter_mut()
            .zip(other.latency_histogram.counts)
        {
            *count += other_count;
        }
    }
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<StressConfig, Box<dyn std::error::Error + Send + Sync>> {
    let mut config = StressConfig::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--addr" => {
                config.server_addr = parse_value(&arg, args.next())?;
            }
            "--connections" => {
                config.connections = parse_value(&arg, args.next())?;
            }
            "--pipeline" => {
                config.pipeline = parse_value(&arg, args.next())?;
            }
            "--requests-per-connection" => {
                config.requests_per_connection = Some(parse_value(&arg, args.next())?);
            }
            "--duration-secs" => {
                let secs: f64 = parse_value(&arg, args.next())?;
                config.duration = Duration::from_secs_f64(secs.max(0.1));
            }
            "--warmup-secs" => {
                let secs: f64 = parse_value(&arg, args.next())?;
                config.warmup = Duration::try_from_secs_f64(secs)
                    .map_err(|_| "warmup seconds must be finite and nonnegative")?;
            }
            "--payload-bytes" => {
                config.payload_bytes = parse_value(&arg, args.next())?;
            }
            "--current-thread" => {
                config.current_thread = true;
            }
            "--blocking-io" => {
                config.blocking_io = true;
            }
            "--tls-insecure" => {
                #[cfg(feature = "rustls")]
                {
                    config.tls_insecure = true;
                }
                #[cfg(not(feature = "rustls"))]
                {
                    return Err("stress client was built without rustls support".into());
                }
            }
            other => {
                return Err(format!("unknown argument: {other}").into());
            }
        }
    }

    if config.connections == 0 {
        return Err("connections must be greater than zero".into());
    }
    if config.pipeline == 0 {
        return Err("pipeline must be greater than zero".into());
    }
    if config.blocking_io && config.pipeline != 1 {
        return Err("--blocking-io requires --pipeline 1".into());
    }
    if config.blocking_io && config.requests_per_connection.is_some() {
        return Err("--blocking-io does not support --requests-per-connection".into());
    }
    if config.requests_per_connection == Some(0) {
        return Err("requests per connection must be greater than zero".into());
    }
    if config.requests_per_connection.is_some() && config.tls_insecure {
        return Err("requests per connection is not supported with TLS".into());
    }

    Ok(config)
}

fn parse_value<T>(
    flag: &str,
    value: Option<String>,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = value.ok_or_else(|| format!("missing value for {flag}"))?;
    Ok(value.parse::<T>()?)
}

fn print_help() {
    println!(
        "Nacelle stress harness\n\
         \n\
         Usage:\n\
           cargo run --release --bin stress -- [options]\n\
         \n\
         Options:\n\
           --addr <addr>          Target server address (default 127.0.0.1:7878)\n\
           --connections <count>  Concurrent TCP connections (default 256)\n\
           --pipeline <count>     Requests kept in flight per connection (default 8)\n\
           --requests-per-connection <count>\n\
                                  Reconnect after this many requests (plain TCP only)\n\
           --duration-secs <s>    Active load duration (default 10)\n\
           --warmup-secs <s>      Unmeasured load on the same established connections\n\
                                  before the measured phase (default 0)\n\
           --payload-bytes <n>    Request payload bytes (default 256)\n\
           --current-thread      Run client tasks and I/O on the main thread\n\
                                  (default: multi-thread Tokio runtime)\n\
           --blocking-io         Use blocking socket/TLS I/O on one additional worker\n\
                                  and current-thread runtime per connection (default off).\n\
                                  Requires --pipeline 1; connection churn is unsupported.\n\
                                  Worker threads and runtime/socket resources scale\n\
                                  with --connections.\n\
           --tls-insecure         Connect with TLS and accept the server certificate\n\
                                  without verification. Use only for the local\n\
                                  self-signed stress server.\n\
         \n\
         Notes:\n\
           - Connections are reused for the whole run unless --requests-per-connection is set.\n\
           - High pipeline values measure saturation throughput and queueing under load, not just base RTT.\n\
           - For established-connection RTT, start with --pipeline 1.\n\
           - RTT includes request frame preparation/write/flush through complete response validation.\n\
           - Initial connects, TLS handshakes, and warmup are excluded from request statistics.\n\
           - Initial connect/handshake timings are setup diagnostics, not steady-state RTT.\n\
           - All responses drain before warmup_completed and measurement_end are flushed.\n\
           - Percentiles include every completion; histogram upper bounds have <0.4% quantization error.\n\
           - Run nacelle-stress-server separately and point --addr at it.\n\
        "
    );
    #[cfg(feature = "phase-timing")]
    println!("Diagnostic build: phase-timing adds client timers; do not use as a CPU baseline.");
}

#[cfg(feature = "phase-timing")]
fn print_phase_summary(result: &WorkerResult) {
    let denominator = result.completed_requests.max(1) as f64;
    println!(
        "client_phase_note    instrumented-only; preparation/write/flush and read/validation; read includes peer wait"
    );
    println!(
        "client_prepare_write_flush_total_ns {}",
        result.preparation_write_flush_ns
    );
    println!(
        "client_response_read_validate_total_ns {}",
        result.response_read_validation_ns
    );
    println!(
        "client_prepare_write_flush_mean {:.2} µs",
        result.preparation_write_flush_ns as f64 / denominator / 1_000.0
    );
    println!(
        "client_response_read_validate_mean {:.2} µs",
        result.response_read_validation_ns as f64 / denominator / 1_000.0
    );
}

fn print_summary(config: &StressConfig, result: &WorkerResult, elapsed: Duration) {
    let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
    let requests_per_sec = result.completed_requests as f64 / elapsed_secs;
    let connections_per_sec = result.completed_connections as f64 / elapsed_secs;
    let total_wire_bytes = result.request_wire_bytes + result.response_wire_bytes;
    let configured_inflight = config.connections.saturating_mul(config.pipeline) as f64;
    let avg_latency_us = if result.completed_requests == 0 {
        0.0
    } else {
        result.latency_total_ns as f64 / result.completed_requests as f64 / 1_000.0
    };
    let little_law_latency_us = if requests_per_sec > 0.0 {
        configured_inflight / requests_per_sec * 1_000_000.0
    } else {
        0.0
    };
    let implied_inflight = requests_per_sec * (avg_latency_us / 1_000_000.0);

    println!();
    println!("completed_connections {}", result.completed_connections);
    println!("connection_rate      {:.2}", connections_per_sec);
    println!("completed_requests   {}", result.completed_requests);
    println!("warmup_requests      {}", result.warmup_completed_requests);
    println!("request_wire_bytes   {}", result.request_wire_bytes);
    println!("response_wire_bytes  {}", result.response_wire_bytes);
    println!("effective_rps        {:.2}", requests_per_sec);
    println!(
        "wire_throughput      {} total ({} / s)",
        format_bytes(total_wire_bytes),
        format_bytes_per_sec(total_wire_bytes, elapsed)
    );
    println!(
        "request_throughput   {} / s",
        format_bytes_per_sec(result.request_wire_bytes, elapsed)
    );
    println!(
        "response_throughput  {} / s",
        format_bytes_per_sec(result.response_wire_bytes, elapsed)
    );
    println!("avg_latency          {:.2} µs", avg_latency_us);
    println!("mean_latency         {:.2} µs", avg_latency_us);
    println!(
        "max_latency          {:.2} µs",
        result.max_latency_ns as f64 / 1_000.0
    );
    if result.completed_requests > 0 {
        println!(
            "p50_latency          {:.2} µs",
            result
                .latency_histogram
                .percentile(result.completed_requests, 50)
                .expect("completed requests have histogram entries") as f64
                / 1_000.0
        );
        println!(
            "p95_latency          {:.2} µs",
            result
                .latency_histogram
                .percentile(result.completed_requests, 95)
                .expect("completed requests have histogram entries") as f64
                / 1_000.0
        );
        println!(
            "p99_latency          {:.2} µs",
            result
                .latency_histogram
                .percentile(result.completed_requests, 99)
                .expect("completed requests have histogram entries") as f64
                / 1_000.0
        );
    }
    println!(
        "client_configured_inflight  {}",
        config.connections * config.pipeline
    );
    println!("client_implied_inflight     {:.1}", implied_inflight);
    println!(
        "client_queue_floor_latency  {:.2} µs",
        little_law_latency_us
    );
    println!(
        "latency_note         full request RTT; all-completion histogram upper bounds (<0.4% error)"
    );
    if config.pipeline > 1 {
        println!(
            "latency_note         saturation mode; use --pipeline 1 for established-connection RTT"
        );
    }
    println!();
    println!(
        "profile              connections={} pipeline={} requests_per_connection={} request_payload={}B elapsed={:.2}s",
        config.connections,
        config.pipeline,
        config
            .requests_per_connection
            .map_or_else(|| "unbounded".to_owned(), |value| value.to_string()),
        config.payload_bytes,
        elapsed_secs,
    );
}

#[cfg(feature = "allocation-counting")]
fn print_allocation_summary(
    allocations: nacelle_stress_common::AllocationSnapshot,
    completed_requests: u64,
) {
    let denominator = completed_requests.max(1) as f64;
    println!(
        "allocation_note      instrumented-only; measured-phase allocator calls, not live bytes"
    );
    println!(
        "allocation_count     {}",
        allocations.successful_allocations()
    );
    println!("allocation_alloc     {}", allocations.alloc_calls);
    println!("allocation_zeroed    {}", allocations.alloc_zeroed_calls);
    println!("allocation_realloc   {}", allocations.realloc_calls);
    println!("allocation_bytes     {}", allocations.requested_bytes);
    println!("allocation_frees     {}", allocations.free_calls);
    println!(
        "allocations_per_request {:.6}",
        allocations.successful_allocations() as f64 / denominator
    );
    println!(
        "allocated_bytes_per_request {:.6}",
        allocations.requested_bytes as f64 / denominator
    );
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes_f64 = bytes as f64;
    if bytes_f64 >= GIB {
        format!("{:.2} GiB", bytes_f64 / GIB)
    } else if bytes_f64 >= MIB {
        format!("{:.2} MiB", bytes_f64 / MIB)
    } else if bytes_f64 >= KIB {
        format!("{:.2} KiB", bytes_f64 / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn format_bytes_per_sec(bytes: u64, elapsed: Duration) -> String {
    let per_sec = bytes as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    format_bytes(per_sec.round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Poll;
    use tokio::io::{AsyncReadExt, DuplexStream};

    const TEST_REQUEST_ID: u64 = 3_u64 << 48;
    const TEST_RESPONSE_END: u32 = 0b0010;
    const TEST_RESPONSE_ERROR: u32 = 0b0100;

    fn response_frame(request_id: u64, flags: u32, payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![0; 24 + payload.len()];
        frame[0..4].copy_from_slice(&(20_u32 + payload.len() as u32).to_le_bytes());
        frame[4..12].copy_from_slice(&request_id.to_le_bytes());
        frame[12..20].copy_from_slice(&STRESS_OPCODE.to_le_bytes());
        frame[20..24].copy_from_slice(&flags.to_le_bytes());
        frame[24..].copy_from_slice(payload);
        frame
    }

    async fn is_pending<F: Future>(mut future: Pin<&mut F>) -> bool {
        std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context).is_pending()))
            .await
    }

    async fn assert_no_next_request(server: &mut DuplexStream) {
        let mut byte = [0];
        let read = server.read(&mut byte);
        tokio::pin!(read);
        assert!(is_pending(read.as_mut()).await, "another request was sent");
    }

    async fn test_requests(
        client: DuplexStream,
        sequential: bool,
        count: usize,
        warmup_barrier: Option<Arc<Barrier>>,
    ) -> Result<WorkerResult, NacelleError> {
        let config = StressConfig {
            pipeline: 1,
            warmup: if warmup_barrier.is_some() {
                Duration::from_millis(1)
            } else {
                Duration::ZERO
            },
            ..StressConfig::default()
        };
        if sequential {
            let mut client = client;
            let (mut io, mut result) = match warmup_barrier {
                Some(barrier) => {
                    prepare_sequential_measurement(3, &mut client, &config, &barrier).await?
                }
                None => (SequentialIo::new(3, &config), WorkerResult::default()),
            };
            run_sequential_io(
                &mut client,
                &mut io,
                &mut result,
                Instant::now() + Duration::from_secs(1),
                count,
            )
            .await?;
            Ok(result)
        } else {
            let (mut reader, mut writer) = tokio::io::split(client);
            let (mut io, mut result) = match warmup_barrier {
                Some(barrier) => {
                    prepare_measurement(3, &mut reader, &mut writer, &config, &barrier).await?
                }
                None => (WorkerIo::new(3, &config), WorkerResult::default()),
            };
            run_worker_io(
                &mut reader,
                &mut writer,
                &mut io,
                &mut result,
                &config,
                Instant::now() + Duration::from_secs(1),
                count,
            )
            .await?;
            Ok(result)
        }
    }

    #[test]
    fn parses_connection_churn_limit() {
        let config = parse_args(["--requests-per-connection".into(), "1".into()])
            .expect("churn limit should parse");

        assert_eq!(config.requests_per_connection, Some(1));
    }

    #[test]
    fn rejects_zero_connection_churn_limit() {
        let error = parse_args(["--requests-per-connection".into(), "0".into()])
            .expect_err("zero churn limit should fail");

        assert_eq!(
            error.to_string(),
            "requests per connection must be greater than zero"
        );
    }
    #[test]
    fn current_thread_runtime_is_explicit_and_preserves_workload() {
        assert!(
            !parse_args(Vec::<String>::new())
                .expect("defaults")
                .current_thread
        );
        let config = parse_args(
            ["--current-thread", "--connections", "2", "--pipeline", "4"].map(str::to_owned),
        )
        .expect("current-thread flag");
        assert!(config.current_thread);
        assert_eq!(config.connections, 2);
        assert_eq!(config.pipeline, 4);
    }

    #[test]
    fn warmup_and_blocking_options_are_opt_in_and_validate_combinations() {
        let defaults = parse_args(Vec::<String>::new()).expect("defaults");
        assert!(!defaults.blocking_io);
        assert_eq!(defaults.warmup, Duration::ZERO);
        let config = parse_args(["--warmup-secs", "0.25"].map(str::to_owned)).expect("warmup flag");
        assert_eq!(config.pipeline, defaults.pipeline);
        assert_eq!(config.warmup, Duration::from_millis(250));
        let blocking = parse_args(
            ["--blocking-io", "--pipeline", "1", "--connections", "2"].map(str::to_owned),
        )
        .expect("blocking flag");
        assert!(blocking.blocking_io);
        assert_eq!(blocking.pipeline, 1);
        assert_eq!(blocking.connections, 2);
        for args in [
            vec!["--blocking-io"],
            vec![
                "--blocking-io",
                "--pipeline",
                "1",
                "--requests-per-connection",
                "1",
            ],
        ] {
            parse_args(args.into_iter().map(str::to_owned))
                .expect_err("blocking pipelines and churn should fail");
        }
        for seconds in ["-1", "NaN", "inf"] {
            assert!(parse_args(["--warmup-secs", seconds].map(str::to_owned)).is_err());
        }
    }

    #[tokio::test]
    async fn pipeline_one_waits_for_multipart_and_fragmented_response_end_in_both_modes() {
        for sequential in [false, true] {
            let (client, mut server) = tokio::io::duplex(1024);
            let worker = test_requests(client, sequential, 2, None);
            tokio::pin!(worker);
            assert!(is_pending(worker.as_mut()).await);
            let mut request = [0; 280];
            server
                .read_exact(&mut request)
                .await
                .expect("first request");
            assert_eq!(&request[0..4], &276_u32.to_le_bytes());
            assert_eq!(&request[4..12], &TEST_REQUEST_ID.to_le_bytes());
            assert_eq!(&request[24..], &[3; 256]);
            assert_no_next_request(&mut server).await;

            server
                .write_all(&response_frame(TEST_REQUEST_ID, 0, &[3; 128]))
                .await
                .expect("first response frame");
            assert!(is_pending(worker.as_mut()).await);
            assert_no_next_request(&mut server).await;
            let last_frame = response_frame(TEST_REQUEST_ID, TEST_RESPONSE_END, &[3; 128]);
            for fragment in [&last_frame[..11], &last_frame[11..last_frame.len() - 1]] {
                server.write_all(fragment).await.expect("response fragment");
                assert!(is_pending(worker.as_mut()).await);
                assert_no_next_request(&mut server).await;
            }
            server
                .write_all(&last_frame[last_frame.len() - 1..])
                .await
                .expect("complete response");
            assert!(is_pending(worker.as_mut()).await);
            server
                .read_exact(&mut request)
                .await
                .expect("second request");
            assert_eq!(&request[4..12], &(TEST_REQUEST_ID + 1).to_le_bytes());
            assert_eq!(&request[24..], &[3; 256]);
            server
                .write_all(&response_frame(
                    TEST_REQUEST_ID + 1,
                    TEST_RESPONSE_END,
                    &[3; 256],
                ))
                .await
                .expect("second response");
            let result = worker.await.expect("requests");
            assert_eq!(result.completed_requests, 2);
            assert_eq!(result.request_wire_bytes, 560);
            assert_eq!(result.response_wire_bytes, 584);
            #[cfg(feature = "phase-timing")]
            assert!(
                result.preparation_write_flush_ns + result.response_read_validation_ns
                    <= result.latency_total_ns
            );
        }
    }

    #[tokio::test]
    async fn sequential_response_errors_match_the_baseline() {
        let cases = [
            (
                Some(response_frame(
                    TEST_REQUEST_ID + 1,
                    TEST_RESPONSE_END,
                    &[3; 256],
                )),
                Some("response request id was not the next in-flight request"),
            ),
            (
                Some(response_frame(
                    TEST_REQUEST_ID,
                    TEST_RESPONSE_ERROR | TEST_RESPONSE_END,
                    &[],
                )),
                Some("stress target returned an error frame"),
            ),
            (None, None),
        ];
        for sequential in [false, true] {
            for (response, expected_reason) in &cases {
                let (client, mut server) = tokio::io::duplex(1024);
                let worker = test_requests(client, sequential, 1, None);
                tokio::pin!(worker);
                assert!(is_pending(worker.as_mut()).await);
                let mut request = [0; 280];
                server.read_exact(&mut request).await.expect("request");
                if let Some(response) = response {
                    server.write_all(response).await.expect("response");
                }
                drop(server);
                let error = worker.await.expect_err("invalid response should fail");
                match (error, expected_reason) {
                    (NacelleError::InvalidFrame(reason), Some(expected)) => {
                        assert_eq!(reason, *expected);
                    }
                    (NacelleError::UnexpectedEof, None) => {}
                    _ => panic!("response validation differed from the baseline"),
                }
            }
        }
    }

    #[test]
    fn histogram_records_short_runs_resets_warmup_and_bounds_quantization() {
        let mut result = WorkerResult::default();
        result.record_completion(1_000_000, 24);
        result.request_wire_bytes = 24;
        result.reset();
        for latency in 1..=20 {
            result.record_completion(latency, 24);
        }
        assert_eq!(result.completed_requests, 20);
        assert_eq!(result.latency_total_ns, 210);
        assert_eq!(result.max_latency_ns, 20);
        assert_eq!(result.request_wire_bytes, 0);
        assert_eq!(result.response_wire_bytes, 480);
        assert_eq!(result.latency_histogram.percentile(20, 95), Some(19));
        for latency in [0, 1, 511, 512, 100_000, u64::MAX] {
            let mut result = WorkerResult::default();
            result.record_completion(latency, 24);
            let upper = result
                .latency_histogram
                .percentile(1, 95)
                .expect("one completion");
            assert!(upper >= latency);
            assert!(upper - latency <= latency / 256);
        }
    }

    #[tokio::test]
    async fn warmup_drains_fragmented_responses_and_reuses_the_measured_connection() {
        for sequential in [false, true] {
            let barrier = Arc::new(Barrier::new(2));
            let (client, mut server) = tokio::io::duplex(1024);
            let worker = tokio::spawn(test_requests(client, sequential, 1, Some(barrier.clone())));
            barrier.wait().await;
            let mut request = [0; 280];
            server
                .read_exact(&mut request)
                .await
                .expect("warmup request");
            tokio::time::sleep(Duration::from_millis(2)).await;
            let response = response_frame(TEST_REQUEST_ID, TEST_RESPONSE_END, &[3; 256]);
            server
                .write_all(&response[..11])
                .await
                .expect("warmup response fragment");
            tokio::task::yield_now().await;
            let warmup_finished = barrier.wait();
            tokio::pin!(warmup_finished);
            assert!(is_pending(warmup_finished.as_mut()).await);
            server
                .write_all(&response[11..])
                .await
                .expect("complete warmup response");
            warmup_finished.await;
            barrier.wait().await;
            server
                .read_exact(&mut request)
                .await
                .expect("measured request");
            assert_eq!(&request[4..12], &(TEST_REQUEST_ID + 1).to_le_bytes());
            server
                .write_all(&response_frame(
                    TEST_REQUEST_ID + 1,
                    TEST_RESPONSE_END,
                    &[3; 256],
                ))
                .await
                .expect("measured response");
            let result = worker.await.expect("worker").expect("measurement");
            assert_eq!(result.completed_requests, 1);
            assert_eq!(result.warmup_completed_requests, 1);
            assert_eq!(result.request_wire_bytes, 280);
            assert_eq!(result.response_wire_bytes, 280);
            #[cfg(feature = "phase-timing")]
            assert!(
                result.preparation_write_flush_ns + result.response_read_validation_ns
                    <= result.latency_total_ns
            );
        }
    }
}
