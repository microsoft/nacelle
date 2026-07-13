//! Trusted-proxy forwarded-IP resolution.

use std::net::IpAddr;

use http::header::HeaderName;
use hyper::Request;
use hyper::body::Incoming;

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
