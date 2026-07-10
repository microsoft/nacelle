use nacelle::{codec, core, runtime};

#[test]
fn common_capability_namespaces_are_available() {
    let _ = core::NacelleLimits::default();
    let _ = codec::LengthDelimitedDecoder::new(1024);
    let _ = runtime::NacelleHost::new();
}

#[cfg(feature = "tcp")]
#[test]
fn tcp_capability_namespace_is_available() {
    let _ = nacelle::tcp::NacelleTcpOptions::default();
}

#[cfg(feature = "http")]
#[test]
fn http_capability_namespace_is_available() {
    let _ = nacelle::http::NacelleHttpLimits::default();
}
