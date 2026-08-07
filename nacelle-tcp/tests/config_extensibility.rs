use std::time::Duration;

use nacelle_tcp::{NacelleTcpConfig, NacelleTcpLimits, ResponseWritePolicy, TcpRequestBodyMode};

#[test]
fn public_tcp_configs_construct_through_defaults_and_builders() {
    let config = NacelleTcpConfig::default()
        .with_read_buffer_capacity(4096)
        .with_response_buffer_capacity(2048)
        .with_max_frame_len(1024)
        .with_request_body_chunk_size(512)
        .with_request_body_channel_capacity(2)
        .with_request_body_mode(TcpRequestBodyMode::Streaming)
        .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered);
    let limits = NacelleTcpLimits::default()
        .with_read_timeout(Duration::from_secs(1))
        .with_write_timeout(Duration::from_secs(2))
        .with_shutdown_timeout(Duration::from_secs(3))
        .with_idle_timeout(Duration::from_secs(4));

    assert_eq!(config.read_buffer_capacity, 4096);
    assert_eq!(config.response_buffer_capacity, 2048);
    assert_eq!(config.max_frame_len, 1024);
    assert_eq!(config.request_body_chunk_size, 512);
    assert_eq!(config.request_body_channel_capacity, 2);
    assert_eq!(config.request_body_mode, TcpRequestBodyMode::Streaming);
    assert_eq!(
        config.response_write_policy,
        ResponseWritePolicy::CoalesceBuffered
    );
    assert_eq!(limits.read_timeout, Some(Duration::from_secs(1)));
    assert_eq!(limits.write_timeout, Some(Duration::from_secs(2)));
    assert_eq!(limits.shutdown_timeout, Some(Duration::from_secs(3)));
    assert_eq!(limits.idle_timeout, Some(Duration::from_secs(4)));
}
