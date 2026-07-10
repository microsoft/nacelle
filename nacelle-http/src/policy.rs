//! HTTP request policy enforcement: host/method/header allowlists and security
//! headers. Wire-level filtering applied before a request reaches the handler.

use http::header::{HeaderName, HeaderValue};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode};
use std::net::IpAddr;

#[derive(Debug, Clone, Default)]
pub struct NacelleHttpPolicy {
    pub(crate) allowed_hosts: Option<Vec<String>>,
    pub(crate) allowed_methods: Option<Vec<Method>>,
    pub(crate) max_uri_len: Option<usize>,
    pub(crate) max_header_count: Option<usize>,
    pub(crate) max_header_bytes: Option<usize>,
    pub(crate) max_requests_per_peer_per_second: Option<usize>,
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
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    let host_without_port = host
        .split_once(':')
        .map(|(host, _port)| host)
        .unwrap_or(host.as_str());
    allowed_hosts
        .iter()
        .any(|allowed| allowed == &host || allowed == host_without_port)
}

pub(crate) fn apply_security_headers(headers: &mut http::HeaderMap, policy: &NacelleHttpPolicy) {
    for (name, value) in &policy.security_headers {
        if !headers.contains_key(name) {
            headers.insert(name.clone(), value.clone());
        }
    }
}
