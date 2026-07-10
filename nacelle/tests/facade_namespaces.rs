use nacelle::{codec, core, runtime};

#[test]
fn common_capability_namespaces_are_available() {
    let _ = core::NacelleLimits::default();
    let _ = codec::LengthDelimitedDecoder::new(1024);
    let _ = runtime::NacelleHost::new();
}

#[test]
fn prelude_contains_common_application_concepts() {
    use nacelle::prelude::*;

    fn accepts_handler<H: Handler>(_handler: H) {}

    accepts_handler(handler_fn(|_request: NacelleRequest| async {
        Ok(NacelleResponse::empty_tcp())
    }));
    let _ = NacelleBody::empty();
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
