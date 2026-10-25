use router::{config::RouterConfig, types::WorkerKey};

#[test]
fn endpoint_is_canonical() {
    let key: WorkerKey = "10.0.0.2:8200".parse().unwrap();
    assert_eq!(key.to_string(), "10.0.0.2:8200");
    assert_eq!(key.metrics(8000).to_string(), "10.0.0.2:8000");
}

#[test]
fn invalid_weights_are_rejected() {
    let mut config = RouterConfig::fixture();
    config.cache_weight = -1.0;
    assert!(config.validate().is_err());
}

#[test]
fn router_requires_token() {
    if std::env::var("WORKLOAD_TOKEN").is_err() {
        assert!(RouterConfig::env().is_err());
    }
}

#[test]
fn adapter_requires_token() {
    if std::env::var("WORKLOAD_TOKEN").is_err() {
        assert!(router::config::AdapterConfig::env().is_err());
    }
}
