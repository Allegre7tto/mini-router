use router::config::RouterConfig;
use router::routing::load::parse_metrics;
use router::types::WorkerKey;
use router::workers::endpoint::Trust;
use std::collections::BTreeSet;

#[test]
fn parse_prometheus_load() {
    let text = r#"vllm:num_requests_running{model_name="fixed"} 2
vllm:num_requests_waiting{model_name="fixed"} 3
vllm:kv_cache_usage_perc{model_name="fixed"} 0.5
"#;
    let m = parse_metrics(text).unwrap();
    assert_eq!(m.running, 2);
    assert_eq!(m.waiting, 3);
    assert_eq!(m.kv_usage, 0.5);
}

#[test]
fn parse_empty_metrics() {
    let m = parse_metrics("").unwrap();
    assert_eq!(m.running, 0);
    assert_eq!(m.waiting, 0);
    assert_eq!(m.kv_usage, 0.0);
}

#[tokio::test]
async fn lifecycle_observes_candidates() {
    let config = RouterConfig::fixture();
    let client = reqwest::Client::new();
    let manager = router::workers::manager::EndpointManager::new(client, config);
    let w1: WorkerKey = "10.0.0.1:8200".parse().unwrap();
    let w2: WorkerKey = "10.0.0.2:8200".parse().unwrap();
    let set = BTreeSet::from([w1.clone(), w2.clone()]);
    let cands = router::types::CandidateSet(set);
    let now = std::time::Instant::now();
    manager.observe(&cands, now).await;
    let snaps = manager.snapshots(&cands).await;
    assert_eq!(snaps.len(), 2);
    for s in &snaps {
        assert_eq!(s.trust, Trust::Warming);
        assert_eq!(s.generation, 1);
    }
}

#[tokio::test]
async fn reservation_and_release() {
    let config = RouterConfig::fixture();
    let client = reqwest::Client::new();
    let manager = router::workers::manager::EndpointManager::new(client, config);
    let w1: WorkerKey = "10.0.0.1:8200".parse().unwrap();
    let set = BTreeSet::from([w1.clone()]);
    let cands = router::types::CandidateSet(set);
    let now = std::time::Instant::now();
    manager.observe(&cands, now).await;

    let did = uuid::Uuid::new_v4();
    let expires = now + std::time::Duration::from_secs(10);
    manager.reserve(did, w1.clone(), expires).await;

    let snaps = manager.snapshots(&cands).await;
    assert_eq!(snaps[0].reservations, 1);

    let result = manager.complete(did, w1.clone()).await;
    assert!(matches!(
        result,
        router::workers::manager::Completion::Released
    ));

    let snaps = manager.snapshots(&cands).await;
    assert_eq!(snaps[0].reservations, 0);
}

#[tokio::test]
async fn reservation_expires() {
    let config = RouterConfig::fixture();
    let client = reqwest::Client::new();
    let manager = router::workers::manager::EndpointManager::new(client, config);
    let w1: WorkerKey = "10.0.0.1:8200".parse().unwrap();
    let set = BTreeSet::from([w1.clone()]);
    let cands = router::types::CandidateSet(set);
    let now = std::time::Instant::now();
    manager.observe(&cands, now).await;

    let did = uuid::Uuid::new_v4();
    let expires = now + std::time::Duration::from_millis(1);
    manager.reserve(did, w1.clone(), expires).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    manager.reap(std::time::Instant::now()).await;

    let snaps = manager.snapshots(&cands).await;
    assert_eq!(snaps[0].reservations, 0);
}

#[tokio::test]
async fn served_mismatch() {
    let config = RouterConfig::fixture();
    let client = reqwest::Client::new();
    let manager = router::workers::manager::EndpointManager::new(client, config);
    let w1: WorkerKey = "10.0.0.1:8200".parse().unwrap();
    let w2: WorkerKey = "10.0.0.2:8200".parse().unwrap();
    let set = BTreeSet::from([w1.clone(), w2.clone()]);
    let cands = router::types::CandidateSet(set);
    let now = std::time::Instant::now();
    manager.observe(&cands, now).await;

    let did = uuid::Uuid::new_v4();
    manager.reserve(did, w1.clone(), now).await;
    let result = manager.complete(did, w2.clone()).await;
    assert!(matches!(
        result,
        router::workers::manager::Completion::Mismatch
    ));
}

#[tokio::test]
async fn ttl_cleanup() {
    let mut config = RouterConfig::fixture();
    config.endpoint_state_ttl_secs = 0;
    let client = reqwest::Client::new();
    let manager = router::workers::manager::EndpointManager::new(client, config);
    let w1: WorkerKey = "10.0.0.1:8200".parse().unwrap();
    let set = BTreeSet::from([w1.clone()]);
    let cands = router::types::CandidateSet(set);
    let now = std::time::Instant::now();
    manager.observe(&cands, now).await;

    let empty = router::types::CandidateSet(BTreeSet::new());
    manager.reap(std::time::Instant::now()).await;

    let snaps = manager.snapshots(&empty).await;
    assert_eq!(snaps.len(), 0);
}
