use nacelle::{codec, core, runtime};

#[test]
fn common_capability_namespaces_are_available() {
    let _ = core::NacelleLimits::default();
    let _ = codec::LengthDelimitedDecoder::new(1024);
    let _ = runtime::NacelleHost::new();
    let _ = runtime::NacelleShutdown::new();
    let _ = std::any::type_name::<nacelle::advanced::runtime::JoinHandle<()>>();
}

#[test]
fn prelude_contains_common_application_concepts() {
    use nacelle::prelude::*;

    let _ = NacelleBody::empty();
    let _ = NacelleApp::new();
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
