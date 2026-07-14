//! Runtime configuration watching and last-known-good reload policy.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nacelle::runtime::NacelleShutdownToken;

use crate::ProxyService;
use crate::config::{ConfigurationSnapshot, ProxyStartupConfiguration, load_snapshot};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReloadPolicy {
    startup: ProxyStartupConfiguration,
}

impl ReloadPolicy {
    pub(crate) const fn new(startup: ProxyStartupConfiguration) -> Self {
        Self { startup }
    }

    fn accepts(&self, candidate: &ConfigurationSnapshot) -> Result<(), &'static str> {
        if candidate.file.startup_configuration() != self.startup {
            return Err("listener and resource limits cannot change at runtime");
        }
        Ok(())
    }
}

pub(crate) async fn watch_configuration(
    config_path: PathBuf,
    mut active: ConfigurationSnapshot,
    policy: ReloadPolicy,
    service: Arc<ProxyService>,
    mut shutdown: NacelleShutdownToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut last_rejection = None;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                match load_snapshot(&config_path).await {
                    Ok(candidate) if candidate != active => {
                        if let Err(error) = policy.accepts(&candidate) {
                            report_rejection(&mut last_rejection, error);
                            continue;
                        }
                        if let Err(error) = reload_tls_if_changed(&active, &candidate, &service) {
                            report_rejection(&mut last_rejection, error);
                            continue;
                        }
                        if let Err(error) = service
                            .set_configuration(candidate.file.runtime_configuration())
                        {
                            report_rejection(&mut last_rejection, error.to_string());
                            continue;
                        }
                        println!("proxy configuration reloaded");
                        active = candidate;
                        last_rejection = None;
                    }
                    Ok(_) => last_rejection = None,
                    Err(error) => {
                        report_rejection(&mut last_rejection, error.to_string());
                    }
                }
            }
            _ = shutdown.changed() => break,
        }
    }
}

fn reload_tls_if_changed(
    active: &ConfigurationSnapshot,
    candidate: &ConfigurationSnapshot,
    service: &ProxyService,
) -> Result<(), String> {
    if candidate.certificate == active.certificate && candidate.private_key == active.private_key {
        return Ok(());
    }

    match (&candidate.certificate, &candidate.private_key) {
        (Some(certificate), Some(private_key)) => service
            .reload_tls(certificate, private_key)
            .map_err(|error| {
                let mut message = format!("invalid TLS material: {error}");
                if let Some(source) = std::error::Error::source(&error) {
                    message.push_str(&format!(": {source}"));
                }
                message
            }),
        (None, None) => Err("TLS file paths cannot be removed at runtime".to_string()),
        _ => unreachable!("configuration validation requires both TLS files"),
    }
}

fn report_rejection(last_rejection: &mut Option<String>, rejection: impl Into<String>) -> bool {
    let rejection = rejection.into();
    if last_rejection.as_ref() == Some(&rejection) {
        return false;
    }
    eprintln!("configuration reload rejected: {rejection}");
    *last_rejection = Some(rejection);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(listen_addr: &str, request_limit: usize) -> ConfigurationSnapshot {
        ConfigurationSnapshot::for_reload_test(listen_addr.parse().expect("address"), request_limit)
    }

    #[test]
    fn rejects_changes_to_startup_only_settings() {
        let initial = snapshot("127.0.0.1:8443", 1024);
        let policy = ReloadPolicy::new(initial.file.startup_configuration());

        assert_eq!(
            policy.accepts(&snapshot("127.0.0.1:9443", 1024)),
            Err("listener and resource limits cannot change at runtime")
        );
        assert_eq!(
            policy.accepts(&snapshot("127.0.0.1:8443", 2048)),
            Err("listener and resource limits cannot change at runtime")
        );
        policy
            .accepts(&snapshot("127.0.0.1:8443", 1024))
            .expect("unchanged startup configuration");
    }

    #[test]
    fn repeated_reload_rejections_are_suppressed() {
        let mut last_rejection = None;

        let first_reported = report_rejection(&mut last_rejection, "invalid certificate");
        let repeated_reported = report_rejection(&mut last_rejection, "invalid certificate");

        assert!(first_reported);
        assert!(!repeated_reported);
    }
}
