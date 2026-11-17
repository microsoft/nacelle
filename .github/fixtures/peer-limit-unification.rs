#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use nacelle::runtime::{ThreadPerCoreLimits, Worker};
    use nacelle_core::{
        NacelleConfigurationError, NacelleConnectionMeta, NacelleError, NacelleLimits,
        NacelleResourceLimitReason,
    };

    #[test]
    fn worker_peer_concurrency_remains_a_process_budget() {
        let peer = SocketAddr::from(([127, 0, 0, 1], 12345));
        for shares in [&[2, 1][..], &[1, 0, 0][..]] {
            let total = shares.iter().sum();
            let policy = ThreadPerCoreLimits::worker(
                NacelleLimits::default().with_max_connections_per_peer(total),
                shares.len(),
            )
            .expect("worker limits");
            let mut admitted = Vec::new();
            for (index, &share) in shares.iter().enumerate() {
                let state = policy
                    .state_for(Worker {
                        index,
                        core_id: index,
                    })
                    .expect("worker state");
                for _ in 0..share {
                    admitted.push(
                        state
                            .register_connection(&NacelleConnectionMeta::tcp(Some(peer), None))
                            .expect("connection within worker share"),
                    );
                }
                assert!(matches!(
                    state.register_connection(&NacelleConnectionMeta::tcp(Some(peer), None)),
                    Err(NacelleError::ResourceLimit(
                        NacelleResourceLimitReason::PeerConnections
                    ))
                ));
            }
            assert_eq!(admitted.len(), total);
        }
    }

    #[test]
    fn worker_peer_rate_tables_remain_a_process_budget() {
        for shares in [&[2, 1][..], &[1, 0][..], &[0, 0][..]] {
            let policy = ThreadPerCoreLimits::worker(
                NacelleLimits::default()
                    .with_connection_rate_limit_table_capacity(shares.iter().sum()),
                shares.len(),
            )
            .expect("worker limits");
            for (index, &share) in shares.iter().enumerate() {
                let state = policy
                    .state_for(Worker {
                        index,
                        core_id: index,
                    })
                    .expect("worker state");
                assert_eq!(
                    state.startup_limits().connection_rate_limit_table_capacity,
                    share
                );
            }
            let result = policy
                .control()
                .set_peer_connection_open_rate(Some(shares.len()));
            if shares.contains(&0) {
                assert!(matches!(
                    result,
                    Err(NacelleError::Configuration(
                        NacelleConfigurationError::ConnectionRateTableCapacity
                    ))
                ));
                assert_eq!(
                    policy
                        .control()
                        .snapshot()
                        .max_connection_opens_per_peer_per_second,
                    None
                );
            } else {
                result.expect("one rate-table slot per worker");
            }
        }
    }
}
