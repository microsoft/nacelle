//! Trusted-proxy forwarded-IP resolution.

use std::net::{IpAddr, SocketAddr};

use http::HeaderMap;

use crate::policy::NacelleForwardedHeader;

pub(crate) struct ForwardedPeerResolution {
    pub(crate) peer_ip: Option<IpAddr>,
    pub(crate) malformed: bool,
}

pub(crate) fn forwarded_peer_ip(
    headers: &HeaderMap,
    header: NacelleForwardedHeader,
    trusted_proxies: &[IpAddr],
) -> ForwardedPeerResolution {
    let name = match header {
        NacelleForwardedHeader::XForwardedFor => "x-forwarded-for",
        NacelleForwardedHeader::Forwarded => "forwarded",
    };
    let mut peer = None;
    let mut malformed = false;
    for value in headers.get_all(name) {
        let Ok(value) = value.to_str() else {
            malformed = true;
            continue;
        };
        for element in value.split(',') {
            let address = match header {
                NacelleForwardedHeader::XForwardedFor => element.trim().parse().ok(),
                NacelleForwardedHeader::Forwarded => parse_forwarded_header_for(element),
            };
            let Some(address) = address else {
                malformed = true;
                continue;
            };
            if peer.is_none() || !trusted_proxies.contains(&address) {
                peer = Some(address);
            }
        }
    }
    ForwardedPeerResolution {
        peer_ip: peer,
        malformed,
    }
}

fn parse_forwarded_header_for(element: &str) -> Option<IpAddr> {
    let mut address = None;
    for part in element.split(';') {
        let (name, value) = part.trim().split_once('=')?;
        if name.trim().eq_ignore_ascii_case("for") {
            if address.is_some() {
                return None;
            }
            let value = value.trim();
            let value = if let Some(quoted) = value.strip_prefix('"') {
                quoted.strip_suffix('"')?
            } else {
                value
            };
            address = Some(parse_forwarded_ip(value)?);
        }
    }
    address
}

fn parse_forwarded_ip(value: &str) -> Option<IpAddr> {
    value
        .parse()
        .ok()
        .or_else(|| value.parse::<SocketAddr>().ok().map(|address| address.ip()))
        .or_else(|| value.strip_prefix('[')?.strip_suffix(']')?.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(header: NacelleForwardedHeader, values: &[(&str, &str)]) -> Option<IpAddr> {
        let mut headers = HeaderMap::new();
        for (name, value) in values {
            headers.append(
                http::header::HeaderName::from_bytes(name.as_bytes()).expect("header name"),
                value.parse().expect("header value"),
            );
        }
        forwarded_peer_ip(&headers, header, &["10.0.0.1".parse().expect("proxy")]).peer_ip
    }

    #[test]
    fn selects_rightmost_untrusted_hop_across_header_fields() {
        for header in [
            NacelleForwardedHeader::XForwardedFor,
            NacelleForwardedHeader::Forwarded,
        ] {
            let values = match header {
                NacelleForwardedHeader::XForwardedFor => [
                    ("x-forwarded-for", "203.0.113.99, 198.51.100.24"),
                    ("x-forwarded-for", "10.0.0.1"),
                ],
                NacelleForwardedHeader::Forwarded => [
                    ("forwarded", "for=203.0.113.99, for=198.51.100.24"),
                    ("forwarded", "for=10.0.0.1;proto=https"),
                ],
            };
            assert_eq!(
                resolve(header, &values),
                Some("198.51.100.24".parse().expect("peer"))
            );
        }
    }

    #[test]
    fn selected_header_never_falls_back_to_another_family() {
        let headers = [
            ("forwarded", "for=203.0.113.99"),
            ("x-forwarded-for", "198.51.100.24"),
        ];
        assert_eq!(
            resolve(NacelleForwardedHeader::XForwardedFor, &headers),
            Some("198.51.100.24".parse().expect("peer"))
        );
        assert_eq!(
            resolve(NacelleForwardedHeader::Forwarded, &headers),
            Some("203.0.113.99".parse().expect("peer"))
        );
        assert_eq!(
            resolve(
                NacelleForwardedHeader::Forwarded,
                &[("x-forwarded-for", "198.51.100.24")]
            ),
            None
        );
        assert_eq!(
            resolve(
                NacelleForwardedHeader::XForwardedFor,
                &[
                    ("forwarded", "for=203.0.113.99"),
                    ("x-forwarded-for", "invalid")
                ]
            ),
            None
        );
    }

    #[test]
    fn malformed_and_ambiguous_addresses_are_ignored() {
        for value in [
            "for=unknown",
            "for=203.0.113.1;for=198.51.100.1",
            "for=\"[::1]junk\"",
            "for=203.0.113.1:bad",
            "for=\"203.0.113.1",
        ] {
            assert_eq!(
                resolve(NacelleForwardedHeader::Forwarded, &[("forwarded", value)]),
                None,
                "{value}"
            );
        }
        for value in ["", "unknown", "203.0.113.1:1234"] {
            assert_eq!(
                resolve(
                    NacelleForwardedHeader::XForwardedFor,
                    &[("x-forwarded-for", value)]
                ),
                None,
                "{value}"
            );
        }
        for (header, value) in [
            (
                NacelleForwardedHeader::Forwarded,
                "for=198.51.100.24, for=unknown",
            ),
            (
                NacelleForwardedHeader::Forwarded,
                "for=198.51.100.24, by=10.0.0.1",
            ),
            (
                NacelleForwardedHeader::XForwardedFor,
                "198.51.100.24, <junk>",
            ),
            (
                NacelleForwardedHeader::XForwardedFor,
                "unknown, 198.51.100.24",
            ),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header_name(header), value.parse().expect("header value"));
            let resolution =
                forwarded_peer_ip(&headers, header, &["10.0.0.1".parse().expect("proxy")]);
            assert_eq!(
                resolution.peer_ip,
                Some("198.51.100.24".parse().expect("peer")),
                "{value}"
            );
            assert!(resolution.malformed, "{value}");
        }
    }

    fn header_name(header: NacelleForwardedHeader) -> &'static str {
        match header {
            NacelleForwardedHeader::XForwardedFor => "x-forwarded-for",
            NacelleForwardedHeader::Forwarded => "forwarded",
        }
    }

    #[test]
    fn supports_numeric_forwarded_addresses_and_all_trusted_chains() {
        for value in ["for=\"[2001:db8::1]:443\"", "for=\"[2001:db8::1]\""] {
            assert_eq!(
                resolve(NacelleForwardedHeader::Forwarded, &[("forwarded", value)]),
                Some("2001:db8::1".parse().expect("ipv6"))
            );
        }
        assert_eq!(
            resolve(
                NacelleForwardedHeader::XForwardedFor,
                &[("x-forwarded-for", "10.0.0.1, 10.0.0.1")]
            ),
            Some("10.0.0.1".parse().expect("proxy"))
        );
    }
}
