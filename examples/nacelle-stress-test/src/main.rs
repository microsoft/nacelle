#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::collections::VecDeque;
use std::fmt::Debug;
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

const LATENCY_SAMPLE_STRIDE: u64 = 64;

#[derive(Debug, Clone)]
struct StressConfig {
    server_addr: SocketAddr,
    connections: usize,
    pipeline: usize,
    duration: Duration,
    payload_bytes: usize,
    tls_insecure: bool,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            server_addr: SocketAddr::from(([127, 0, 0, 1], 7878)),
            connections: 256,
            pipeline: 8,
            duration: Duration::from_secs(10),
            payload_bytes: 256,
            tls_insecure: false,
        }
    }
}

#[derive(Debug, Default)]
struct WorkerResult {
    completed_requests: u64,
    request_wire_bytes: u64,
    response_wire_bytes: u64,
    latency_total_ns: u128,
    max_latency_ns: u64,
    latency_samples_ns: Vec<u64>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args(std::env::args().skip(1))?;

    println!(
        "stress target={} connections={} pipeline={} request_payload={}B duration={}s",
        config.server_addr,
        config.connections,
        config.pipeline,
        config.payload_bytes,
        config.duration.as_secs_f64(),
    );
    if config.tls_insecure {
        println!("stress transport=tcp-tls verify=insecure");
    }

    let barrier = Arc::new(Barrier::new(config.connections + 1));
    let mut workers = Vec::with_capacity(config.connections);
    for worker_id in 0..config.connections {
        workers.push(tokio::spawn(run_worker(
            worker_id,
            config.server_addr,
            config.clone(),
            barrier.clone(),
        )));
    }

    barrier.wait().await;
    let load_started = Instant::now();
    let mut aggregate = WorkerResult::default();
    for worker in workers {
        aggregate.merge(worker.await??);
    }
    let total_elapsed = load_started.elapsed();

    print_summary(&config, &aggregate, total_elapsed);
    Ok(())
}

async fn run_worker(
    worker_id: usize,
    server_addr: SocketAddr,
    config: StressConfig,
    barrier: Arc<Barrier>,
) -> Result<WorkerResult, NacelleError> {
    let stream = make_tcp_socket(&server_addr)?.connect(server_addr).await?;
    stream.set_nodelay(true)?;
    if config.tls_insecure {
        #[cfg(feature = "rustls")]
        {
            let connector = tokio_rustls::TlsConnector::from(Arc::new(insecure_tls_config()));
            let server_name = ServerName::try_from("localhost")
                .map_err(|_| NacelleError::InvalidFrame("invalid TLS server name"))?;
            let stream = connector.connect(server_name, stream).await?;
            let (reader, writer) = tokio::io::split(stream);
            return run_worker_io(worker_id, reader, writer, config, barrier).await;
        }
        #[cfg(not(feature = "rustls"))]
        {
            return Err(NacelleError::InvalidFrame(
                "stress client was built without rustls support",
            ));
        }
    }

    let (reader, writer) = stream.into_split();
    run_worker_io(worker_id, reader, writer, config, barrier).await
}

async fn run_worker_io<R, W>(
    worker_id: usize,
    mut reader: R,
    mut writer: W,
    config: StressConfig,
    barrier: Arc<Barrier>,
) -> Result<WorkerResult, NacelleError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let payload = vec![worker_id as u8; config.payload_bytes];
    let mut request_frame = vec![0_u8; 24 + payload.len()];
    request_frame[24..].copy_from_slice(&payload);
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut inflight = VecDeque::with_capacity(config.pipeline);
    let mut result = WorkerResult::default();
    let mut next_request_id = (worker_id as u64) << 48;

    barrier.wait().await;
    let deadline = Instant::now() + config.duration;

    while inflight.len() < config.pipeline {
        if Instant::now() >= deadline {
            break;
        }
        let request_id = next_request_id;
        next_request_id += 1;
        result.request_wire_bytes +=
            write_request_frame(&mut writer, request_id, STRESS_OPCODE, &mut request_frame).await?
                as u64;
        inflight.push_back((request_id, Instant::now()));
    }

    while !inflight.is_empty() {
        let response = read_response(&mut reader, &mut read_buf).await?;
        let (expected_request_id, started_at) = inflight.pop_front().ok_or(
            NacelleError::InvalidFrame("response completed without an in-flight request"),
        )?;
        if expected_request_id != response.request_id {
            return Err(NacelleError::InvalidFrame(
                "response request id was not the next in-flight request",
            ));
        }

        let latency_ns = started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        result.record_completion(latency_ns, response.wire_bytes as u64);

        if Instant::now() < deadline {
            let request_id = next_request_id;
            next_request_id += 1;
            result.request_wire_bytes +=
                write_request_frame(&mut writer, request_id, STRESS_OPCODE, &mut request_frame)
                    .await? as u64;
            inflight.push_back((request_id, Instant::now()));
        }
    }

    writer.shutdown().await?;
    Ok(result)
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
        if self
            .completed_requests
            .is_multiple_of(LATENCY_SAMPLE_STRIDE)
        {
            self.latency_samples_ns.push(latency_ns);
        }
    }

    fn merge(&mut self, other: WorkerResult) {
        self.completed_requests += other.completed_requests;
        self.request_wire_bytes += other.request_wire_bytes;
        self.response_wire_bytes += other.response_wire_bytes;
        self.latency_total_ns += other.latency_total_ns;
        self.max_latency_ns = self.max_latency_ns.max(other.max_latency_ns);
        self.latency_samples_ns.extend(other.latency_samples_ns);
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
            "--duration-secs" => {
                let secs: f64 = parse_value(&arg, args.next())?;
                config.duration = Duration::from_secs_f64(secs.max(0.1));
            }
            "--payload-bytes" => {
                config.payload_bytes = parse_value(&arg, args.next())?;
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
           --duration-secs <s>    Active load duration (default 10)\n\
           --payload-bytes <n>    Request payload bytes (default 256)\n\
           --tls-insecure         Connect with TLS and accept the server certificate\n\
                                  without verification. Use only for the local\n\
                                  self-signed stress server.\n\
         \n\
         Notes:\n\
           - Connections are established once per worker and reused for the whole run.\n\
           - High pipeline values measure saturation throughput and queueing under load, not just base RTT.\n\
           - For established-connection RTT, start with --pipeline 1.\n\
           - Run nacelle-stress-server separately and point --addr at it.\n\
        "
    );
}

fn print_summary(config: &StressConfig, result: &WorkerResult, elapsed: Duration) {
    let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
    let requests_per_sec = result.completed_requests as f64 / elapsed_secs;
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
    let mut samples = result.latency_samples_ns.clone();
    samples.sort_unstable();

    println!();
    println!("completed_requests   {}", result.completed_requests);
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
    println!(
        "max_latency          {:.2} µs",
        result.max_latency_ns as f64 / 1_000.0
    );
    if !samples.is_empty() {
        println!(
            "p50_latency          {:.2} µs",
            percentile(&samples, 0.50) as f64 / 1_000.0
        );
        println!(
            "p95_latency          {:.2} µs",
            percentile(&samples, 0.95) as f64 / 1_000.0
        );
        println!(
            "p99_latency          {:.2} µs",
            percentile(&samples, 0.99) as f64 / 1_000.0
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
    if config.pipeline > 1 {
        println!(
            "latency_note         saturation mode; use --pipeline 1 for established-connection RTT"
        );
    }
    println!();
    println!(
        "profile              connections={} pipeline={} request_payload={}B elapsed={:.2}s",
        config.connections, config.pipeline, config.payload_bytes, elapsed_secs,
    );
}

fn percentile(sorted_samples: &[u64], percentile: f64) -> u64 {
    let index = ((sorted_samples.len().saturating_sub(1)) as f64 * percentile).round() as usize;
    sorted_samples[index]
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
