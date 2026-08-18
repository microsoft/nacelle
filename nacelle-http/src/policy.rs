//! HTTP request policy enforcement: host/method/header allowlists and security
//! headers. Wire-level filtering applied before a request reaches the handler.

use http::header::{HeaderName, HeaderValue};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode};
use nacelle_core::DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY;
use std::net::{IpAddr, Ipv6Addr};

#[derive(Debug, Clone)]
pub struct NacelleHttpPolicy {
    pub(crate) allowed_hosts: Option<Vec<String>>,
    pub(crate) allowed_methods: Option<Vec<Method>>,
    pub(crate) max_uri_len: Option<usize>,
    pub(crate) max_header_count: Option<usize>,
    pub(crate) max_header_bytes: Option<usize>,
    pub(crate) max_requests_per_peer_per_second: Option<usize>,
    pub(crate) peer_rate_limit_table_capacity: usize,
    pub(crate) trusted_proxy_ips: Option<Vec<IpAddr>>,
    pub(crate) security_headers: Vec<(HeaderName, HeaderValue)>,
}

impl NacelleHttpPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_allowed_hosts(
        mut self,
        hosts: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.allowed_hosts = Some(
            hosts
                .into_iter()
                .map(|host| host.into().trim_end_matches('.').to_ascii_lowercase())
                .collect(),
        );
        self
    }

    pub fn with_allowed_methods(mut self, methods: impl IntoIterator<Item = Method>) -> Self {
        self.allowed_methods = Some(methods.into_iter().collect());
        self
    }

    pub fn with_max_uri_len(mut self, max: usize) -> Self {
        self.max_uri_len = Some(max);
        self
    }

    pub fn with_max_header_count(mut self, max: usize) -> Self {
        self.max_header_count = Some(max);
        self
    }

    pub fn with_max_header_bytes(mut self, max: usize) -> Self {
        self.max_header_bytes = Some(max);
        self
    }

    pub fn with_max_requests_per_peer_per_second(mut self, max: usize) -> Self {
        self.max_requests_per_peer_per_second = Some(max.max(1));
        self
    }

    /// Set the maximum number of peers retained by the HTTP request-rate limiter.
    ///
    /// When the bounded table is full or cannot find an inactive entry within
    /// its fixed probe budget, newly observed peers receive a rate-limit
    /// rejection. This bound applies only when
    /// [`Self::with_max_requests_per_peer_per_second`] is enabled.
    pub fn with_peer_rate_limit_table_capacity(mut self, capacity: usize) -> Self {
        self.peer_rate_limit_table_capacity = capacity.max(1);
        self
    }

    pub fn with_trusted_proxy_ips(mut self, ips: impl IntoIterator<Item = IpAddr>) -> Self {
        self.trusted_proxy_ips = Some(ips.into_iter().collect());
        self
    }

    pub fn with_security_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.security_headers.push((name, value));
        self
    }

    pub fn with_default_security_headers(self) -> Self {
        self.with_security_header(
            http::header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        )
        .with_security_header(
            HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("deny"),
        )
        .with_security_header(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("no-referrer"),
        )
        .with_security_header(
            HeaderName::from_static("cross-origin-resource-policy"),
            HeaderValue::from_static("same-origin"),
        )
    }

    pub fn with_strict_transport_security(mut self, value: HeaderValue) -> Self {
        self.security_headers
            .push((http::header::STRICT_TRANSPORT_SECURITY, value));
        self
    }
}

impl Default for NacelleHttpPolicy {
    fn default() -> Self {
        Self {
            allowed_hosts: None,
            allowed_methods: None,
            max_uri_len: None,
            max_header_count: None,
            max_header_bytes: None,
            max_requests_per_peer_per_second: None,
            peer_rate_limit_table_capacity: DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY,
            trusted_proxy_ips: None,
            security_headers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HttpRejection {
    pub(crate) status: StatusCode,
    pub(crate) reason: &'static str,
}

pub(crate) fn validate_http_policy(
    policy: &NacelleHttpPolicy,
    request: &Request<Incoming>,
) -> Option<HttpRejection> {
    if let Some(max_uri_len) = policy.max_uri_len
        && request
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str().len())
            .unwrap_or_else(|| request.uri().path().len())
            > max_uri_len
    {
        return Some(HttpRejection {
            status: StatusCode::URI_TOO_LONG,
            reason: "uri_too_long",
        });
    }

    if let Some(methods) = &policy.allowed_methods
        && !methods.iter().any(|method| method == request.method())
    {
        return Some(HttpRejection {
            status: StatusCode::METHOD_NOT_ALLOWED,
            reason: "method_not_allowed",
        });
    }

    if let Some(max_header_count) = policy.max_header_count
        && request.headers().len() > max_header_count
    {
        return Some(HttpRejection {
            status: StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            reason: "header_count",
        });
    }

    if let Some(max_header_bytes) = policy.max_header_bytes {
        let header_bytes = request
            .headers()
            .iter()
            .try_fold(0_usize, |total, (name, value)| {
                total
                    .checked_add(name.as_str().len())?
                    .checked_add(value.as_bytes().len())
            });
        if header_bytes.is_none_or(|bytes| bytes > max_header_bytes) {
            return Some(HttpRejection {
                status: StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
                reason: "header_bytes",
            });
        }
    }

    if let Some(hosts) = &policy.allowed_hosts
        && !host_allowed(hosts, request)
    {
        return Some(HttpRejection {
            status: StatusCode::MISDIRECTED_REQUEST,
            reason: "host",
        });
    }

    None
}

fn host_allowed(allowed_hosts: &[String], request: &Request<Incoming>) -> bool {
    let Some(host) = request
        .headers()
        .get(http::header::HOST)
        .and_then(|host| host.to_str().ok())
    else {
        return false;
    };
    host_header_allowed(allowed_hosts, host)
}

/// Match a raw `Host` header value against the (already normalized) allowlist.
///
/// The header is parsed as an HTTP authority so IPv6 literals such as
/// `[::1]` and `[::1]:8080` are handled correctly instead of being truncated at
/// the first colon. A malformed authority never matches.
fn host_header_allowed(allowed_hosts: &[String], host: &str) -> bool {
    let Some(normalized) = normalize_host(host) else {
        return false;
    };
    let host_with_port = normalized
        .port
        .map(|port| format!("{}:{}", normalized.host, port));
    allowed_hosts.iter().any(|allowed| {
        allowed == &normalized.host || host_with_port.as_deref() == Some(allowed.as_str())
    })
}

/// A `Host` authority split into a normalized host and optional port.
struct NormalizedHost {
    /// Host without a port. IPv6 literals keep their surrounding brackets and
    /// are canonicalized; registered names are lowercased with any trailing dot
    /// removed.
    host: String,
    /// Numeric port when the authority carried one.
    port: Option<u16>,
}

/// Parse a `Host` header value into a normalized host and optional port.
///
/// Returns `None` for a malformed authority (unbalanced brackets, an invalid
/// IPv6 literal, a bare IPv6 address without brackets, or a bad port).
fn normalize_host(value: &str) -> Option<NormalizedHost> {
    // IPv6 literal: `[<addr>]` optionally followed by `:<port>`.
    if let Some(rest) = value.strip_prefix('[') {
        let close = rest.find(']')?;
        let addr: Ipv6Addr = rest[..close].parse().ok()?;
        let port = match &rest[close + 1..] {
            "" => None,
            suffix => Some(parse_port(suffix.strip_prefix(':')?)?),
        };
        return Some(NormalizedHost {
            host: format!("[{addr}]"),
            port,
        });
    }

    // Registered name or IPv4 literal, with an optional trailing `:<port>`. A
    // bare IPv6 literal (multiple colons without brackets) is malformed.
    let (name, port) = match value.rsplit_once(':') {
        Some((name, _)) if name.contains(':') => return None,
        Some((name, port)) => (name, Some(parse_port(port)?)),
        None => (value, None),
    };
    let host = name.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    Some(NormalizedHost { host, port })
}

fn parse_port(port: &str) -> Option<u16> {
    if port.is_empty() {
        return None;
    }
    port.parse().ok()
}

pub(crate) fn apply_security_headers(headers: &mut http::HeaderMap, policy: &NacelleHttpPolicy) {
    for (name, value) in &policy.security_headers {
        if !headers.contains_key(name) {
            headers.insert(name.clone(), value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an allowlist the same way [`NacelleHttpPolicy::with_allowed_hosts`]
    /// normalizes configured entries.
    fn allowlist(entries: &[&str]) -> Vec<String> {
        entries
            .iter()
            .map(|entry| entry.trim_end_matches('.').to_ascii_lowercase())
            .collect()
    }

    #[test]
    fn ipv6_literal_without_port_is_allowed() {
        let hosts = allowlist(&["[::1]"]);
        assert!(host_header_allowed(&hosts, "[::1]"));
    }

    #[test]
    fn ipv6_literal_with_port_matches_host_only_entry() {
        let hosts = allowlist(&["[::1]"]);
        assert!(host_header_allowed(&hosts, "[::1]:8080"));
    }

    #[test]
    fn ipv6_literal_matches_port_qualified_entry() {
        let hosts = allowlist(&["[::1]:8080"]);
        assert!(host_header_allowed(&hosts, "[::1]:8080"));
        // A port-qualified entry must not match the bare host.
        assert!(!host_header_allowed(&hosts, "[::1]"));
        // Nor a different port.
        assert!(!host_header_allowed(&hosts, "[::1]:9090"));
    }

    #[test]
    fn ipv6_literal_is_canonicalized_before_matching() {
        let hosts = allowlist(&["[::1]"]);
        assert!(host_header_allowed(&hosts, "[0:0:0:0:0:0:0:1]"));
        assert!(host_header_allowed(&hosts, "[::0001]:8080"));
    }

    #[test]
    fn dns_name_matches_with_and_without_port() {
        let hosts = allowlist(&["example.com"]);
        assert!(host_header_allowed(&hosts, "example.com"));
        assert!(host_header_allowed(&hosts, "example.com:8080"));
        assert!(host_header_allowed(&hosts, "EXAMPLE.COM"));
    }

    #[test]
    fn dns_name_port_qualified_entry() {
        let hosts = allowlist(&["example.com:8080"]);
        assert!(host_header_allowed(&hosts, "example.com:8080"));
        assert!(!host_header_allowed(&hosts, "example.com"));
        assert!(!host_header_allowed(&hosts, "example.com:9090"));
    }

    #[test]
    fn trailing_dot_dns_name_is_normalized() {
        let hosts = allowlist(&["example.com"]);
        assert!(host_header_allowed(&hosts, "example.com."));
        assert!(host_header_allowed(&hosts, "example.com.:8080"));
    }

    #[test]
    fn ipv4_literal_matches_with_and_without_port() {
        let hosts = allowlist(&["127.0.0.1"]);
        assert!(host_header_allowed(&hosts, "127.0.0.1"));
        assert!(host_header_allowed(&hosts, "127.0.0.1:8080"));
    }

    #[test]
    fn malformed_authorities_are_rejected() {
        let hosts = allowlist(&["[::1]", "example.com"]);
        assert!(!host_header_allowed(&hosts, "[::1"), "unclosed bracket");
        assert!(
            !host_header_allowed(&hosts, "[::1]junk"),
            "junk after bracket"
        );
        assert!(!host_header_allowed(&hosts, "[not-ipv6]"), "invalid ipv6");
        assert!(!host_header_allowed(&hosts, "[]"), "empty ipv6");
        assert!(
            !host_header_allowed(&hosts, "::1"),
            "bare ipv6 without brackets"
        );
        assert!(
            !host_header_allowed(&hosts, "fe80::1"),
            "bare ipv6 with port-like tail"
        );
        assert!(!host_header_allowed(&hosts, "example.com:"), "empty port");
        assert!(
            !host_header_allowed(&hosts, "example.com:99999"),
            "port out of range"
        );
        assert!(
            !host_header_allowed(&hosts, "example.com:abc"),
            "non-numeric port"
        );
    }

    #[test]
    fn disallowed_hosts_are_rejected() {
        let hosts = allowlist(&["example.com"]);
        assert!(!host_header_allowed(&hosts, "evil.example"));
        assert!(!host_header_allowed(&hosts, "[::1]"));
        assert!(!host_header_allowed(&hosts, "sub.example.com"));
    }
}
