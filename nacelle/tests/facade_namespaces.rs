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
    let _ = std::any::type_name::<nacelle::tcp::SerialTcpServer<(), ()>>();
    let _ = std::any::type_name::<nacelle::tcp::LocalSerialTcpServer<(), ()>>();

    #[allow(dead_code)]
    fn serial_traits_are_public<P, H, LH, OH, LOH>()
    where
        P: nacelle::tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle::tcp::SerialTcpHandler<P>,
        LH: nacelle::tcp::LocalSerialTcpHandler<P>,
        OH: nacelle::tcp::SerialTcpOneWayHandler<P>,
        LOH: nacelle::tcp::LocalSerialTcpOneWayHandler<P>,
    {
        let _ = std::marker::PhantomData::<(H, LH, OH, LOH)>;
        let _ = std::any::type_name::<nacelle::tcp::SerialTcpRequestContext<'static, P>>();
        let _ = std::any::type_name::<nacelle::tcp::SerialTcpOneWayContext<'static, P>>();
    }

    #[allow(dead_code)]
    fn shared_protocol_is_public<P: nacelle::tcp::SharedProtocol>() {}

    // Importing the generic runner is the compile assertion; there is no
    // meaningful function item until a concrete protocol family is supplied.
    #[allow(unused_imports)]
    use nacelle::runtime::run_local_serial_tcp_thread_per_core;
    #[cfg(feature = "openssl")]
    #[allow(unused_imports)]
    use nacelle::runtime::{
        run_local_serial_tcp_openssl_thread_per_core,
        run_local_serial_tcp_optional_openssl_thread_per_core,
    };
}

#[cfg(feature = "http")]
#[test]
fn http_capability_namespace_is_available() {
    let _ = nacelle::http::NacelleHttpLimits::default();
}
