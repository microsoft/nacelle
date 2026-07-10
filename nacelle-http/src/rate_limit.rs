//! Per-peer HTTP rate limiting and trusted-proxy forwarded-IP resolution.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use http::header::HeaderName;
use hyper::Request;
use hyper::body::Incoming;

#[derive(Debug, Clone)]
pub(crate) struct PeerRateWindow {
    started_at: Instant,
    count: usize,
}

/// Returns `true` if a request from `peer_ip` is within the per-second budget,
/// recording the request against the current window. Evicts stale windows.
pub(crate) fn allow_peer_request(
    peer_rate_limits: &Mutex<HashMap<IpAddr, PeerRateWindow>>,
    limit: usize,
    peer_ip: IpAddr,
) -> bool {
    let now = Instant::now();
    let mut peers = peer_rate_limits
        .lock()
        .expect("HTTP peer rate map poisoned");
    peers.retain(|_peer, window| now.duration_since(window.started_at) < Duration::from_secs(60));
    let window = peers.entry(peer_ip).or_insert(PeerRateWindow {
        started_at: now,
        count: 0,
    });
    if now.duration_since(window.started_at) >= Duration::from_secs(1) {
        window.started_at = now;
        window.count = 0;
    }
    if window.count >= limit {
        return false;
    }
    window.count += 1;
    true
}

pub(crate) fn forwarded_peer_ip(request: &Request<Incoming>) -> Option<IpAddr> {
    request
        .headers()
        .get(http::header::FORWARDED)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_forwarded_header_for)
        .or_else(|| {
            request
                .headers()
                .get(HeaderName::from_static("x-forwarded-for"))
                .and_then(|value| value.to_str().ok())
                .and_then(parse_x_forwarded_for)
        })
}

fn parse_forwarded_header_for(value: &str) -> Option<IpAddr> {
    let first = value.split(',').next()?.trim();
    for part in first.split(';') {
        let (name, value) = part.trim().split_once('=')?;
        if name.trim().eq_ignore_ascii_case("for") {
            return parse_forwarded_ip(value.trim().trim_matches('"'));
        }
    }
    None
}

fn parse_x_forwarded_for(value: &str) -> Option<IpAddr> {
    parse_forwarded_ip(value.split(',').next()?.trim())
}

fn parse_forwarded_ip(value: &str) -> Option<IpAddr> {
    let value = value.trim();
    if let Ok(ip) = value.parse::<IpAddr>() {
        return Some(ip);
    }
    if let Some(stripped) = value.strip_prefix('[')
        && let Some((ip, _rest)) = stripped.split_once(']')
    {
        return ip.parse().ok();
    }
    if let Some((ip, _port)) = value.rsplit_once(':')
        && !ip.contains(':')
    {
        return ip.parse().ok();
    }
    None
}
