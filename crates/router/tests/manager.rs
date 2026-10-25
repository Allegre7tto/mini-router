use router::config::RouterConfig;
use router::kv::events::{CacheEvent, EventBatch};
use router::kv::index::PrefixIndex;
use router::kv::subscriber::Event;
use router::routing::load::parse_metrics;
use router::telemetry::RouterMetrics;
use router::types::WorkerKey;
use router::workers::endpoint::Trust;
use router::workers::manager::EndpointManager;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_manager(
    config: RouterConfig,
) -> (
    EndpointManager,
    Arc<Mutex<PrefixIndex>>,
    mpsc::Receiver<(WorkerKey, Event)>,
) {
    let index = Arc::new(Mutex::new(PrefixIndex::default()));
    let (tx, rx) = mpsc::channel(16);
    (
        EndpointManager::new(
            reqwest::Client::new(),
            config,
            index.clone(),
            tx,
            Arc::new(RouterMetrics::new().unwrap()),
        ),
        index,
        rx,
    )
}

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
    assert!(parse_metrics("").is_err());
}

#[tokio::test]
async fn refreshes_load() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/metrics"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            "vllm:num_requests_running{model_name=\"fixed\"} 2\n\
             vllm:num_requests_waiting{model_name=\"fixed\"} 3\n\
             vllm:kv_cache_usage_perc{model_name=\"fixed\"} 0.5\n",
        ))
        .mount(&server)
        .await;

    let mut config = RouterConfig::fixture();
    config.metrics_port = server.address().port();
    let (manager, _index, _rx) = make_manager(config);
    let worker = WorkerKey(std::net::SocketAddr::new(server.address().ip(), 8200));
    let cands = router::types::CandidateSet(BTreeSet::from([worker.clone()]));
    manager.observe(&cands, std::time::Instant::now()).await;
    manager.refresh().await;

    let load = manager.snapshots(&cands).await[0].load.clone().unwrap();
    assert_eq!(load.running, 2);
    assert_eq!(load.waiting, 3);
    assert_eq!(load.kv_usage, 0.5);
}

#[tokio::test]
async fn lifecycle_observes_candidates() {
    let config = RouterConfig::fixture();
    let (manager, _index, _rx) = make_manager(config);
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
    let (manager, _index, _rx) = make_manager(config);
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
    let (manager, _index, _rx) = make_manager(config);
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
    let (manager, _index, _rx) = make_manager(config);
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
    let (manager, _index, _rx) = make_manager(config);
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

#[tokio::test]
async fn handle_trust_transitions() {
    let config = RouterConfig::fixture();
    let (manager, _index, _rx) = make_manager(config);
    let w1: WorkerKey = "10.0.0.1:8200".parse().unwrap();
    let set = BTreeSet::from([w1.clone()]);
    let cands = router::types::CandidateSet(set);
    let now = std::time::Instant::now();
    manager.observe(&cands, now).await;

    let batch = EventBatch {
        ts: 1.0,
        events: vec![CacheEvent::Cleared],
        data_parallel_rank: None,
    };
    manager
        .handle(w1.clone(), Event::Batch(batch.clone()))
        .await;
    let snap = manager.snapshots(&cands).await.remove(0);
    assert_eq!(snap.trust, Trust::Trusted);
    assert_eq!(snap.generation, 1);

    manager.handle(w1.clone(), Event::Gap).await;
    let snap = manager.snapshots(&cands).await.remove(0);
    assert_eq!(snap.trust, Trust::Untrusted);

    manager.handle(w1.clone(), Event::Replayed).await;
    let snap = manager.snapshots(&cands).await.remove(0);
    assert_eq!(snap.trust, Trust::Trusted);

    manager.handle(w1.clone(), Event::Reset).await;
    let snap = manager.snapshots(&cands).await.remove(0);
    assert_eq!(snap.trust, Trust::Warming);
    assert_eq!(snap.generation, 2);
}
